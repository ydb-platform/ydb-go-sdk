package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
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

func (m *mockConn) Prepare(string) (driver.Stmt, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockConn) PrepareContext(ctx context.Context, sql string) (driver.Stmt, error) {
	m.t.Log(stack.Record(0))
	if m.closed {
		return nil, driver.ErrBadConn
	}

	return &mockStmt{
		t:    m.t,
		conn: m,
		sql:  sql,
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

func (m *mockConn) QueryContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Rows, error) {
	m.t.Log(stack.Record(0))
	if xerrors.MustDeleteTableOrQuerySession(m.execErr) {
		m.closed = true
	}

	return nil, m.queryErr
}

func (m *mockConn) ExecContext(ctx context.Context, sql string, args []driver.NamedValue) (driver.Result, error) {
	m.t.Log(stack.Record(0))
	if xerrors.MustDeleteTableOrQuerySession(m.execErr) {
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
	t    testing.TB
	conn *mockConn
	sql  string
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

	return m.conn.ExecContext(ctx, m.sql, args)
}

func (m *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	m.t.Log(stack.Record(0))

	return nil, driver.ErrSkip
}

func (m *mockStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	m.t.Log(stack.Record(0))

	return m.conn.QueryContext(ctx, m.sql, args)
}

func TestDoTx(t *testing.T) {
	for _, idempotentType := range []idempotency{
		idempotent,
		nonIdempotent,
	} {
		t.Run(idempotentType.String(), func(t *testing.T) {
			for i, tt := range errsToCheck {
				t.Run(strconv.Itoa(i)+"."+tt.err.Error(), func(t *testing.T) {
					m := &mockConnector{
						t:        t,
						queryErr: badconn.Map(tt.err),
						execErr:  badconn.Map(tt.err),
					}
					db := sql.OpenDB(m)
					var attempts int
					err := DoTx(context.Background(), db,
						func(ctx context.Context, tx *sql.Tx) error {
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
						},
						WithIdempotent(bool(idempotentType)),
						WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
						WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
					)
					if tt.canRetry[idempotentType] {
						require.NoError(t, err)
						require.NotEmpty(t, attempts)
						if xerrors.Is(m.queryErr, driver.ErrBadConn) {
							require.Greater(t, m.conns, uint32(1))
						} else {
							require.Equal(t, uint32(1), m.conns)
						}
					} else {
						require.Error(t, err)
					}
				})
			}
		})
	}
}

func TestCleanUpResourcesOnPanicInRetryOperation(t *testing.T) {
	panicErr := errors.New("test")
	t.Run("Do", func(t *testing.T) {
		m := &mockConnector{
			t: t,
		}
		db := sql.OpenDB(m)
		defer func() {
			require.NoError(t, db.Close())
		}()
		require.Panics(t, func() {
			require.Equal(t, 0, db.Stats().OpenConnections)
			require.Equal(t, 0, db.Stats().Idle)
			require.Equal(t, 0, db.Stats().InUse)
			defer func() {
				require.Equal(t, 1, db.Stats().OpenConnections)
				require.Equal(t, 1, db.Stats().Idle)
				require.Equal(t, 0, db.Stats().InUse)
			}()
			_ = Do(context.Background(), db,
				func(ctx context.Context, cc *sql.Conn) error {
					require.Equal(t, 1, db.Stats().OpenConnections)
					require.Equal(t, 0, db.Stats().Idle)
					require.Equal(t, 1, db.Stats().InUse)
					panic(panicErr)
				},
			)
		})
	})
	t.Run("DoTx", func(t *testing.T) {
		m := &mockConnector{
			t: t,
		}
		db := sql.OpenDB(m)
		defer func() {
			require.NoError(t, db.Close())
		}()
		require.Panics(t, func() {
			require.Equal(t, 0, db.Stats().OpenConnections)
			require.Equal(t, 0, db.Stats().Idle)
			require.Equal(t, 0, db.Stats().InUse)
			defer func() {
				require.Equal(t, 1, db.Stats().OpenConnections)
				require.Equal(t, 1, db.Stats().Idle)
				require.Equal(t, 0, db.Stats().InUse)
			}()
			_ = DoTx(context.Background(), db,
				func(ctx context.Context, tx *sql.Tx) error {
					require.Equal(t, 1, db.Stats().OpenConnections)
					require.Equal(t, 0, db.Stats().Idle)
					require.Equal(t, 1, db.Stats().InUse)
					panic(panicErr)
				},
			)
		})
	})
}
