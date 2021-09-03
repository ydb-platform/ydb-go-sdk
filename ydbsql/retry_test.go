package ydbsql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/YandexDatabase/ydb-go-sdk/v3"
	"google.golang.org/protobuf/proto"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YandexDatabase/ydb-go-genproto/protos/Ydb_Table"
	"github.com/YandexDatabase/ydb-go-sdk/v3/table"
	"github.com/YandexDatabase/ydb-go-sdk/v3/testutil"
)

type ClusterBuilder struct {
	Logf  func(string, ...interface{})
	Error func(context.Context, testutil.MethodCode) error
}

func (b *ClusterBuilder) log(msg string, args ...interface{}) {
	if b.Logf == nil {
		return
	}
	b.Logf(fmt.Sprint("db stub: ", fmt.Sprintf(msg, args...)))
}

func (b *ClusterBuilder) Build() ydb.Cluster {
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
	return testutil.NewCluster(
		testutil.WithInvokeHandlers(
			testutil.InvokeHandlers{
				testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
					sid := fmt.Sprintf("ydb://test-session/%d", atomic.AddInt32(&sessionID, 1))
					b.log("[%q] create session", sid)

					mu.Lock()
					sessions[sid] = new(session)
					mu.Unlock()

					return &Ydb_Table.CreateSessionResult{
						SessionId: sid,
					}, nil
				},
				testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
					sid := request.(*Ydb_Table.KeepAliveRequest).SessionId
					b.log("[%q] keepalive session", sid)

					mu.RLock()
					s := sessions[sid]
					mu.RUnlock()

					s.Lock()
					s.busy = false
					s.Unlock()

					return &Ydb_Table.KeepAliveResult{
						SessionStatus: Ydb_Table.KeepAliveResult_SESSION_STATUS_READY,
					}, nil
				},
				testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
					sid := request.(*Ydb_Table.DeleteSessionRequest).SessionId
					b.log("[%q] delete session", sid)

					mu.Lock()
					delete(sessions, sid)
					mu.Unlock()

					return nil, nil
				},
				testutil.TableBeginTransaction: func(request interface{}) (result proto.Message, err error) {
					sid := request.(*Ydb_Table.BeginTransactionRequest).SessionId
					tid := fmt.Sprintf("test-tx/%d", atomic.AddInt32(&txID, 1))

					b.log("[%q][%q] begin transaction", sid, tid)

					return &Ydb_Table.BeginTransactionResult{
						TxMeta: &Ydb_Table.TransactionMeta{
							Id: tid,
						},
					}, nil
				},
				testutil.TableCommitTransaction: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.CommitTransactionRequest)
					sid := r.SessionId
					tid := r.TxId

					b.log("[%q][%q] commit transaction", sid, tid)

					return &Ydb_Table.CommitTransactionResult{}, nil
				},
				testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.RollbackTransactionRequest)
					sid := r.SessionId
					tid := r.TxId

					b.log("[%q][%q] rollback transaction", sid, tid)

					return nil, nil
				},
				testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.PrepareDataQueryRequest)
					sid := r.SessionId
					b.log("[%q] prepare data query", sid)
					mu.RLock()
					s := sessions[sid]
					mu.RUnlock()

					s.Lock()
					s.busy = true
					s.Unlock()
					if b.Error != nil {
						err = b.Error(nil, testutil.TablePrepareDataQuery)
					}
					return &Ydb_Table.PrepareQueryResult{}, err
				},
				testutil.TableExecuteDataQuery: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.ExecuteDataQueryRequest)
					sid := r.SessionId
					tid := r.TxControl.TxSelector.(*Ydb_Table.TransactionControl_TxId).TxId
					b.log("[%q][%q] execute data query", sid, tid)
					return &Ydb_Table.ExecuteQueryResult{
						TxMeta: &Ydb_Table.TransactionMeta{
							Id: tid,
						},
					}, err
				},
			},
		),
	)
}

func TestTxDoerStmt(t *testing.T) {
	var count int
	b := ClusterBuilder{
		Error: func(_ context.Context, method testutil.MethodCode) (err error) {
			defer func() { count++ }()
			if count > 0 && count < 3 {
				err = &ydb.TransportError{
					Reason: ydb.TransportErrorDeadlineExceeded,
				}
			}
			return
		},
		Logf: t.Logf,
	}
	cluster := b.Build()

	busyChecking := make(chan bool)
	db := sql.OpenDB(Connector(
		WithSessionPoolIdleThreshold(time.Hour),
		WithSessionPoolTrace(table.SessionPoolTrace{
			OnBusyCheck: func(info table.SessionPoolBusyCheckStartInfo) func(table.SessionPoolBusyCheckDoneInfo) {
				busyChecking <- true
				t.Logf("busy checking session %q", info.Session.ID)
				return nil
			},
		}),
		WithClient(table.NewClient(cluster)),
	))
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "QUERY")

	_, err = db.PrepareContext(ctx, "QUERY")

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

	// Try to repeat the same thing – we should not receive any error here –
	// previous session must be marked busy and not used for some time.
	err = DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.Stmt(stmt).Exec()
		return err
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
