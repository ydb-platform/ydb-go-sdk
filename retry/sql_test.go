//go:build go1.18
// +build go1.18

package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
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

func (m *mockConnector) Open(name string) (driver.Conn, error) {
	m.t.Log(xerrors.StackRecord(0))
	return nil, driver.ErrSkip
}

func (m *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	m.t.Log(xerrors.StackRecord(0))
	m.conns++
	return &mockConn{
		t:        m.t,
		queryErr: m.queryErr,
		execErr:  m.execErr,
	}, nil
}

func (m *mockConnector) Driver() driver.Driver {
	m.t.Log(xerrors.StackRecord(0))
	return m
}

type mockConn struct {
	t        testing.TB
	queryErr error
	execErr  error
	closed   bool
}

func (m *mockConn) Prepare(query string) (driver.Stmt, error) {
	m.t.Log(xerrors.StackRecord(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}
	return &mockStmt{
		t:    m.t,
		conn: m,
	}, nil
}

func (m *mockConn) Close() error {
	m.t.Log(xerrors.StackRecord(0))
	m.closed = true
	return nil
}

func (m *mockConn) Begin() (driver.Tx, error) {
	m.t.Log(xerrors.StackRecord(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}
	return m, nil
}

func (m *mockConn) Exec(args []driver.Value) (driver.Result, error) {
	m.t.Log(xerrors.StackRecord(0))
	if retry.MustDeleteSession(m.execErr) {
		m.closed = true
	}
	return nil, m.execErr
}

func (m *mockConn) Query(args []driver.Value) (driver.Rows, error) {
	m.t.Log(xerrors.StackRecord(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}
	if retry.MustDeleteSession(m.queryErr) {
		m.closed = true
	}
	return nil, m.queryErr
}

func (m *mockConn) Commit() error {
	m.t.Log(xerrors.StackRecord(0))
	return nil
}

func (m *mockConn) Rollback() error {
	m.t.Log(xerrors.StackRecord(0))
	return nil
}

type mockStmt struct {
	t    testing.TB
	conn *mockConn
}

func (m *mockStmt) Close() error {
	m.t.Log(xerrors.StackRecord(0))
	return nil
}

func (m *mockStmt) NumInput() int {
	m.t.Log(xerrors.StackRecord(0))
	return 0
}

func (m *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	m.t.Log(xerrors.StackRecord(0))
	return m.conn.Exec(args)
}

func (m *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	m.t.Log(xerrors.StackRecord(0))
	return m.conn.Query(args)
}

//nolint:nestif
func TestDoTx(t *testing.T) {
	for _, idempotentType := range []idempotency{
		idempotent,
		nonIdempotent,
	} {
		t.Run(idempotentType.String(), func(t *testing.T) {
			for _, tt := range checkErrs {
				t.Run(tt.err.Error(), func(t *testing.T) {
					m := &mockConnector{
						t:        t,
						queryErr: badconn.Map(tt.err),
						execErr:  badconn.Map(tt.err),
					}
					db := sql.OpenDB(m)
					var attempts int
					err := DoTx(context.Background(), db, func(ctx context.Context, tx *sql.Tx) error {
						attempts++
						if attempts > 10 {
							return nil
						}
						rows, err := tx.QueryContext(ctx, "SELECT 1")
						if err != nil {
							return err
						}
						return rows.Err()
					}, WithRetryOptions(
						WithIdempotent(bool(idempotentType)),
						WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithTrace(trace.Retry{
							//nolint:lll
							OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
								t.Logf("attempt %d, conn %d, mode: %+v", attempts, m.conns, Check(m.queryErr))
								return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
									t.Logf("attempt %d, conn %d, mode: %+v", attempts, m.conns, Check(m.queryErr))
									return nil
								}
							},
						}),
					))
					if tt.canRetry[idempotentType] {
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
					} else if err == nil {
						t.Errorf("unexpected nil err (attempts=%d, driver conns=%d)", attempts, m.conns)
					}
				})
			}
		})
	}
}
