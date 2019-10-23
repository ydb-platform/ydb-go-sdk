package ydbsql

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/api/protos/Ydb_Table"
	"github.com/yandex-cloud/ydb-go-sdk/table"
	"github.com/yandex-cloud/ydb-go-sdk/testutil"
)

type DriverBuilder struct {
	Logf  func(string, ...interface{})
	Error func(context.Context, testutil.MethodCode) error
}

func (b *DriverBuilder) log(msg string, args ...interface{}) {
	if b.Logf == nil {
		return
	}
	b.Logf(fmt.Sprint("db stub: ", fmt.Sprintf(msg, args...)))
}

func (b *DriverBuilder) Build() ydb.Driver {
	type session struct {
		sync.Mutex
		busy bool
	}
	var (
		mu        sync.RWMutex
		sessionID int32
		txID      int32

		sessions = map[string]*session{}
	)
	return &testutil.Driver{
		OnCall: func(ctx context.Context, method testutil.MethodCode, req, res interface{}) (err error) {
			var (
				sid string
				tid string
			)
			switch method {
			case testutil.TableCreateSession:
				sid = fmt.Sprintf("ydb://test-session/%d", atomic.AddInt32(&sessionID, 1))
				b.log("[%q] create session", sid)

				mu.Lock()
				sessions[sid] = new(session)
				mu.Unlock()

				r := testutil.TableCreateSessionResult{res}
				r.SetSessionID(sid)

			case testutil.TableKeepAlive:
				sid = req.(*Ydb_Table.KeepAliveRequest).SessionId
				b.log("[%q] keepalive session", sid)

				mu.RLock()
				s := sessions[sid]
				mu.RUnlock()

				s.Lock()
				s.busy = false
				s.Unlock()

				r := testutil.TableKeepAliveResult{res}
				r.SetSessionStatus(true)

				return

			case testutil.TableDeleteSession:
				sid = req.(*Ydb_Table.DeleteSessionRequest).SessionId
				b.log("[%q] delete session", sid)

				mu.Lock()
				delete(sessions, sid)
				mu.Unlock()

				return

			case testutil.TableBeginTransaction:
				sid = req.(*Ydb_Table.BeginTransactionRequest).SessionId
				tid = fmt.Sprintf("test-tx/%d", atomic.AddInt32(&txID, 1))

				r := testutil.TableBeginTransactionResult{res}
				r.SetTransactionID(tid)

				b.log("[%q][%q] begin transaction", sid, tid)

			case testutil.TableCommitTransaction:
				r := req.(*Ydb_Table.CommitTransactionRequest)
				sid = r.SessionId
				tid = r.TxId

				b.log("[%q][%q] commit transaction", sid, tid)

			case testutil.TableRollbackTransaction:
				r := req.(*Ydb_Table.RollbackTransactionRequest)
				sid = r.SessionId
				tid = r.TxId

				b.log("[%q][%q] rollback transaction", sid, tid)

			case testutil.TablePrepareDataQuery:
				r := req.(*Ydb_Table.PrepareDataQueryRequest)
				sid = r.SessionId

				b.log("[%q] prepare data query", sid)

			case testutil.TableExecuteDataQuery:
				{
					r := testutil.TableExecuteDataQueryRequest{req}
					sid = r.SessionID()
					tid, _ = r.TransactionID()
				}
				{
					r := testutil.TableExecuteDataQueryResult{res}
					r.SetTransactionID(tid)
				}

				b.log("[%q][%q] execute data query", sid, tid)

			default:
				return fmt.Errorf("db stub: not implemented")
			}

			mu.RLock()
			s := sessions[sid]
			mu.RUnlock()

			if s == nil {
				return &ydb.OpError{
					Reason: ydb.StatusSessionExpired,
				}
			}

			s.Lock()
			defer s.Unlock()

			if s.busy {
				return &ydb.OpError{
					Reason: ydb.StatusPreconditionFailed,
				}
			}
			if b.Error != nil {
				err = b.Error(ctx, method)
			}

			s.busy = isBusy(err)

			return
		},
	}
}

func TestTxDoerStmt(t *testing.T) {
	var count int
	b := DriverBuilder{
		Error: func(_ context.Context, method testutil.MethodCode) (err error) {
			if method != testutil.TablePrepareDataQuery {
				return nil
			}
			defer func() { count++ }()
			if count == 1 {
				err = &ydb.TransportError{
					Reason: ydb.TransportErrorDeadlineExceeded,
				}
			}
			return
		},
		Logf: t.Logf,
	}
	driver := b.Build()

	busyChecking := make(chan struct{})
	db := sql.OpenDB(Connector(
		WithSessionPoolIdleThreshold(time.Hour),
		WithSessionPoolTrace(table.SessionPoolTrace{
			BusyCheckStart: func(info table.SessionPoolBusyCheckStartInfo) {
				busyChecking <- struct{}{}
				t.Logf("busy checking session %q", info.Session.ID)
			},
		}),
		WithClient(&table.Client{
			Driver: driver,
		}),
	))
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "QUERY")
	if err != nil {
		t.Fatal(err)
	}

	// Block previously created session to force one more session to be created
	// and one more prepared statement be done.
	c, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Try to prepare statement on second session, which must fail due to our
	// stub logic above.
	err = DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Stmt(stmt).Exec()
		return err
	})
	if !isBusy(err) {
		t.Fatalf("not busy error: %v", err)
	}

	const timeout = time.Second
	select {
	case <-busyChecking:
	case <-time.After(timeout):
		t.Fatalf("no busy checking after %s", timeout)
	}

	// Try to repeate the same thing – we should not receive any error here –
	// previous session must be marked busy and not used for some time.
	err = DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Stmt(stmt).Exec()
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
