package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type mockConnector struct {
	t        testing.TB
	conns    uint32
	queryErr error
	execErr  error
}

var _ driver.Connector = &mockConnector{}

func (m *mockConnector) Open(name string) (driver.Conn, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	m.t.Log(stack.Record(0))
	m.conns++

	return &mockConn{
		t:        m.t,
		queryErr: m.queryErr,
		execErr:  m.execErr,
	}, nil
}

func (m *mockConnector) Driver() driver.Driver {
	m.t.Log(stack.Record(0))

	return m
}

type mockConn struct {
	t        testing.TB
	queryErr error
	execErr  error
	closed   bool
}

var (
	_ driver.Conn               = &mockConn{}
	_ driver.ConnPrepareContext = &mockConn{}
	_ driver.ConnBeginTx        = &mockConn{}
	_ driver.ExecerContext      = &mockConn{}
	_ driver.QueryerContext     = &mockConn{}
)

func (m *mockConn) Prepare(query string) (driver.Stmt, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	m.t.Log(stack.Record(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}

	return &mockStmt{
		t:     m.t,
		conn:  m,
		query: query,
	}, nil
}

func (m *mockConn) Close() error {
	m.t.Log(stack.Record(0))
	m.closed = true

	return nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	m.t.Log(stack.Record(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}

	return m, nil
}

func (m *mockConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	m.t.Log(stack.Record(0))
	if xerrors.MustDeleteSession(m.execErr) {
		m.closed = true
	}

	return nil, m.queryErr
}

func (m *mockConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	m.t.Log(stack.Record(0))
	if xerrors.MustDeleteSession(m.execErr) {
		m.closed = true
	}

	return nil, m.execErr
}

func (m *mockConn) Commit() error {
	m.t.Log(stack.Record(0))

	return nil
}

func (m *mockConn) Rollback() error {
	m.t.Log(stack.Record(0))

	return nil
}

type mockStmt struct {
	t     testing.TB
	conn  *mockConn
	query string
}

var (
	_ driver.Stmt             = &mockStmt{}
	_ driver.StmtExecContext  = &mockStmt{}
	_ driver.StmtQueryContext = &mockStmt{}
)

func (m *mockStmt) Close() error {
	m.t.Log(stack.Record(0))

	return nil
}

func (m *mockStmt) NumInput() int {
	m.t.Log(stack.Record(0))

	return -1
}

func (m *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	m.t.Log(stack.Record(0))

	return m.conn.ExecContext(ctx, m.query, args)
}

func (m *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	m.t.Log(stack.Record(0))

	return m.conn.QueryContext(ctx, m.query, args)
}

func TestDoTx(t *testing.T) {
	for _, idempotentType := range []idempotency{
		idempotent,
		nonIdempotent,
	} {
		t.Run(idempotentType.String(), func(t *testing.T) {
			for _, tt := range errsToCheck {
				t.Run(tt.err.Error(), func(t *testing.T) {
					m := &mockConnector{
						t:        t,
						queryErr: badconn.Map(tt.err),
						execErr:  badconn.Map(tt.err),
					}
					db := sql.OpenDB(m)
					var attempts int
					err := DoTx(context.Background(), db,
						withTestSelectOne(attempts),
						WithIdempotent(bool(idempotentType)),
						WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithTrace(&trace.Retry{
							//nolint:lll
							OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
								t.Logf("attempt %d, conn %d, mode: %+v", attempts, m.conns, Check(m.queryErr))

								return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
									t.Logf("attempt %d, conn %d, mode: %+v", attempts, m.conns, Check(m.queryErr))

									return nil
								}
							},
						}),
					)
					if tt.canRetry[idempotentType] {
						validateRetry(t, err, attempts, m, tt)
					} else if err == nil {
						t.Errorf("unexpected nil err (attempts=%d, driver conns=%d)", attempts, m.conns)
					}
				})
			}
		})
	}
}

// validateRetry is a function that validates the retry behavior of a transaction.
func validateRetry(
	t *testing.T,
	err error,
	attempts int,
	m *mockConnector,
	tt struct {
		err           error
		backoff       backoff.Type
		deleteSession bool
		canRetry      map[idempotency]bool
	},
) {
	if err != nil {
		t.Errorf("unexpected err after attempts=%d and driver conns=%d: %v)", attempts, m.conns, err)
	}
	if attempts <= 1 {
		t.Errorf("must be attempts > 1 (actual=%d), driver conns=%d)", attempts, m.conns)
	}
	if tt.deleteSession {
		if m.conns <= 1 {
			t.Errorf("must be retry on different conns (attempts=%d, driver conns=%d)", attempts, m.conns)
		}
	} else {
		if m.conns > 1 {
			t.Errorf("must be retry on single conn (attempts=%d, driver conns=%d)", attempts, m.conns)
		}
	}
}

// withTestSelectOne is a function that returns a closure. The closure takes a context and a *sql.Tx
// as arguments and performs a query on the transaction. It is used to execute a query on a database.
func withTestSelectOne(attempts int) func(ctx context.Context, tx *sql.Tx) error {
	return func(ctx context.Context, tx *sql.Tx) error {
		attempts++
		if attempts > 10 {
			return nil
		}
		rows, err := tx.QueryContext(ctx, "SELECT 1")
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()

		return rows.Err()
	}
}
