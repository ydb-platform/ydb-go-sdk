package xsql

import (
	"context"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// errYdbErrors lists representative YDB errors that should always be mapped to
// driver.ErrBadConn when they escape the xsql proxy layer.
var errYdbErrors = []error{
	xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)),
	xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY)),
	xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED)),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "unavailable")),
	xerrors.Transport(grpcStatus.Error(grpcCodes.Internal, "internal")),
	context.DeadlineExceeded,
	context.Canceled,
}

// mockErrConn is a mock common.Conn that returns a configurable error on every
// operation so we can test that the proxy layer applies badconn.Map correctly.
type mockErrConn struct {
	err error
}

func (m *mockErrConn) ID() string     { return "err-conn" }
func (m *mockErrConn) NodeID() uint32 { return 0 }
func (m *mockErrConn) IsValid() bool  { return true }

func (m *mockErrConn) Ping(_ context.Context) error { return m.err }

func (m *mockErrConn) BeginTx(_ context.Context, _ driver.TxOptions) (common.Tx, error) {
	return nil, m.err
}

func (m *mockErrConn) Close() error { return m.err }

func (m *mockErrConn) Query(
	_ context.Context, _ string, _ *params.Params,
) (common.Rows, error) {
	return nil, m.err
}

func (m *mockErrConn) Exec(
	_ context.Context, _ string, _ *params.Params,
) (driver.Result, error) {
	return nil, m.err
}

func (m *mockErrConn) Explain(
	_ context.Context, _ string, _ *params.Params,
) (string, string, error) {
	return "", "", m.err
}

// mockErrTx is a mock common.Tx that returns a configurable error on every operation.
type mockErrTx struct {
	err error
}

func (m *mockErrTx) ID() string { return "err-tx" }

func (m *mockErrTx) Commit(_ context.Context) error { return m.err }

func (m *mockErrTx) Rollback(_ context.Context) error { return m.err }

func (m *mockErrTx) Exec(
	_ context.Context, _ string, _ *params.Params,
) (driver.Result, error) {
	return nil, m.err
}

func (m *mockErrTx) Query(
	_ context.Context, _ string, _ *params.Params,
) (common.Rows, error) {
	return nil, m.err
}

// mockErrProcessor is a processor (used in Stmt) that returns a configurable error.
type mockErrProcessor struct {
	err error
}

func (m *mockErrProcessor) Exec(
	_ context.Context, _ string, _ *params.Params,
) (driver.Result, error) {
	return nil, m.err
}

func (m *mockErrProcessor) Query(
	_ context.Context, _ string, _ *params.Params,
) (common.Rows, error) {
	return nil, m.err
}

func newErrConn(err error) *Conn {
	return &Conn{
		cc: &mockErrConn{err: err},
		connector: &Connector{
			trace:     &trace.DatabaseSQL{},
			bindings:  newMockBindings(),
			clock:     clockwork.NewRealClock(),
			processor: QUERY,
		},
		ctx: context.Background(),
	}
}

// TestConnProxy_BadConnMapping verifies that each Conn method wraps YDB errors
// with badconn.Map so that errors.Is(err, driver.ErrBadConn) is true for
// session-invalidating errors.
func TestConnProxy_BadConnMapping(t *testing.T) {
	for _, ydbErr := range errYdbErrors {
		wantBadConn := xerrors.MustDeleteTableOrQuerySession(ydbErr)
		t.Run(ydbErr.Error(), func(t *testing.T) {
			t.Run("Ping", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				err := conn.Ping(context.Background())
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("Close", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				err := conn.Close()
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("BeginTx", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				_, err := conn.BeginTx(context.Background(), driver.TxOptions{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("QueryContext_NoTx", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				_, err := conn.QueryContext(context.Background(), "SELECT 1", []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("ExecContext_NoTx", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				_, err := conn.ExecContext(context.Background(), "SELECT 1", []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("QueryContext_WithTx", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				conn.currentTx = &Tx{tx: &mockErrTx{err: ydbErr}}
				_, err := conn.QueryContext(context.Background(), "SELECT 1", []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("ExecContext_WithTx", func(t *testing.T) {
				conn := newErrConn(ydbErr)
				conn.currentTx = &Tx{tx: &mockErrTx{err: ydbErr}}
				_, err := conn.ExecContext(context.Background(), "SELECT 1", []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})
		})
	}
}

// TestTxProxy_BadConnMapping verifies that each Tx method wraps YDB errors
// with badconn.Map.
func TestTxProxy_BadConnMapping(t *testing.T) {
	for _, ydbErr := range errYdbErrors {
		wantBadConn := xerrors.MustDeleteTableOrQuerySession(ydbErr)
		t.Run(ydbErr.Error(), func(t *testing.T) {
			newTx := func() *Tx {
				conn := &Conn{
					cc: &mockCommonConn{},
					connector: &Connector{
						trace:    &trace.DatabaseSQL{},
						bindings: newMockBindings(),
					},
					ctx: context.Background(),
				}
				tx := &Tx{
					conn: conn,
					tx:   &mockErrTx{err: ydbErr},
					ctx:  context.Background(),
				}
				conn.currentTx = tx

				return tx
			}

			t.Run("Commit", func(t *testing.T) {
				err := newTx().Commit()
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("Rollback", func(t *testing.T) {
				err := newTx().Rollback()
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("QueryContext", func(t *testing.T) {
				_, err := newTx().QueryContext(
					context.Background(), "SELECT 1", []driver.NamedValue{},
				)
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("ExecContext", func(t *testing.T) {
				_, err := newTx().ExecContext(
					context.Background(), "SELECT 1", []driver.NamedValue{},
				)
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})
		})
	}
}

// TestStmtProxy_BadConnMapping verifies that each Stmt method wraps YDB errors
// with badconn.Map.
func TestStmtProxy_BadConnMapping(t *testing.T) {
	for _, ydbErr := range errYdbErrors {
		wantBadConn := xerrors.MustDeleteTableOrQuerySession(ydbErr)
		t.Run(ydbErr.Error(), func(t *testing.T) {
			newStmt := func() *Stmt {
				return &Stmt{
					conn: &Conn{
						cc: &mockCommonConn{},
						connector: &Connector{
							trace:    &trace.DatabaseSQL{},
							bindings: newMockBindings(),
						},
						ctx: context.Background(),
					},
					processor: &mockErrProcessor{err: ydbErr},
					ctx:       context.Background(),
					sql:       "SELECT 1",
				}
			}

			t.Run("QueryContext", func(t *testing.T) {
				_, err := newStmt().QueryContext(context.Background(), []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("ExecContext", func(t *testing.T) {
				_, err := newStmt().ExecContext(context.Background(), []driver.NamedValue{})
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})
		})
	}
}

type mockErrRows struct {
	err error
}

func (m *mockErrRows) ColumnTypeDatabaseTypeName(_ context.Context, _ int) string { return "" }
func (m *mockErrRows) ColumnTypeNullable(_ context.Context, _ int) (nullable, ok bool) {
	return false, false
}
func (m *mockErrRows) Columns(context.Context) []string { return nil }
func (m *mockErrRows) Close() error                     { return m.err }
func (m *mockErrRows) HasNextResultSet(context.Context) bool {
	return false
}
func (m *mockErrRows) Next(context.Context, []driver.Value) error { return m.err }
func (m *mockErrRows) NextResultSet(context.Context) error        { return m.err }

// TestRows_BadConnMapping verifies that the Rows wrapper applies
// badconn.Map to errors from Next and NextResultSet.
func TestRows_BadConnMapping(t *testing.T) {
	for _, ydbErr := range errYdbErrors {
		wantBadConn := xerrors.MustDeleteTableOrQuerySession(ydbErr)
		t.Run(ydbErr.Error(), func(t *testing.T) {
			rows := newRows(t.Context(), &mockErrRows{err: ydbErr})

			t.Run("Next", func(t *testing.T) {
				err := rows.Next(nil)
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("NextResultSet", func(t *testing.T) {
				rows := newRows(t.Context(), &mockErrRows{err: ydbErr})
				err := rows.(interface{ NextResultSet() error }).NextResultSet()
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})

			t.Run("Close", func(t *testing.T) {
				rows := newRows(t.Context(), &mockErrRows{err: ydbErr})
				err := rows.Close()
				require.Equal(t, wantBadConn, xerrors.Is(err, driver.ErrBadConn))
			})
		})
	}
}

func TestRows_IOEOFPreserved(t *testing.T) {
	rows := newRows(t.Context(), &mockErrRows{err: io.EOF})
	err := rows.Next(nil)
	require.Equal(t, io.EOF, err)

	rows = newRows(t.Context(), &mockErrRows{err: io.EOF})
	err = rows.(interface{ NextResultSet() error }).NextResultSet()
	require.Equal(t, io.EOF, err)
}
