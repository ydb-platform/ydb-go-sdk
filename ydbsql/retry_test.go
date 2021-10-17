package ydbsql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
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

func (b *ClusterBuilder) Build() cluster.Cluster {
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
				// nolint:unparam
				testutil.TableCreateSession: func(_ interface{}) (result proto.Message, err error) {
					sid := fmt.Sprintf("ydb://test-session/%d", atomic.AddInt32(&sessionID, 1))
					b.log("[%q] create session", sid)

					mu.Lock()
					sessions[sid] = new(session)
					mu.Unlock()

					return &Ydb_Table.CreateSessionResult{
						SessionId: sid,
					}, nil
				},
				// nolint:unparam
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
				// nolint:unparam
				testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
					sid := request.(*Ydb_Table.DeleteSessionRequest).SessionId
					b.log("[%q] delete session", sid)

					mu.Lock()
					delete(sessions, sid)
					mu.Unlock()

					return nil, nil
				},
				// nolint:unparam
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
				// nolint:unparam
				testutil.TableCommitTransaction: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.CommitTransactionRequest)
					sid := r.SessionId
					tid := r.TxId

					b.log("[%q][%q] commit transaction", sid, tid)

					return &Ydb_Table.CommitTransactionResult{}, nil
				},
				// nolint:unparam
				testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.RollbackTransactionRequest)
					sid := r.SessionId
					tid := r.TxId

					b.log("[%q][%q] rollback transaction", sid, tid)

					return result, err
				},
				// nolint:unparam
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
				// nolint:unparam
				testutil.TableExecuteDataQuery: func(request interface{}) (result proto.Message, err error) {
					r := request.(*Ydb_Table.ExecuteDataQueryRequest)
					sid := r.SessionId
					tid := r.TxControl.TxSelector.(*Ydb_Table.TransactionControl_TxId).TxId
					b.log("[%q][%q] execute data query", sid, tid)
					return &Ydb_Table.ExecuteQueryResult{
						TxMeta: &Ydb_Table.TransactionMeta{
							Id: tid,
						},
					}, nil
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
				err = &errors.TransportError{
					Reason: errors.TransportErrorDeadlineExceeded,
				}
			}
			return
		},
		Logf: t.Logf,
	}
	c := b.Build()

	connector, err := Connector(
		With(
			ydb.With(
				config.WithEndpoint("127.0.0.1:9999"),
				config.WithNetDial(func(ctx context.Context, s string) (net.Conn, error) {
					return nil, nil
				}),
			),
		),
		withClient(table.New(context.Background(), c)),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	db := sql.OpenDB(connector)
	if err = db.Ping(); err != nil {
		t.Fatal(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stmt, err := db.PrepareContext(ctx, "QUERY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to prepare statement on second session, which must fail due to our
	// stub logic above.
	err = DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
		_, err = tx.Stmt(stmt).Exec()
		return
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to repeat the same thing – we should not receive any error here –
	// previous session must be marked busy and not used for some time.
	err = DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
		_, err = tx.Stmt(stmt).Exec()
		return
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
