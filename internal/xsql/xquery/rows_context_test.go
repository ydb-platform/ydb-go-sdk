package xquery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xiter"
)

var (
	_ driver.Driver         = sqlStubDriver{}
	_ driver.Connector      = sqlStubConnector{}
	_ driver.Conn           = (*sqlStubConn)(nil)
	_ driver.QueryerContext = (*sqlStubConn)(nil)

	// driver Rows surface for database/sql exercised by stubs below.
	_ driver.Rows                           = (*ctxForwardingDriverRows)(nil)
	_ driver.RowsNextResultSet              = (*ctxForwardingDriverRows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*ctxForwardingDriverRows)(nil)
	_ driver.RowsColumnTypeNullable         = (*ctxForwardingDriverRows)(nil)
)

// ctxForwardingDriverRows attaches sql QueryContext to xquery.rows without importing internal/xsql
// (avoids package cycles against the facade).
type ctxForwardingDriverRows struct {
	ctx context.Context //nolint:containedctx

	inner *rows
}

func newCtxForwardingDriverRows(ctx context.Context, inner *rows) *ctxForwardingDriverRows {
	return &ctxForwardingDriverRows{ctx: ctx, inner: inner}
}

func (d *ctxForwardingDriverRows) Close() error {
	return d.inner.Close()
}

func (d *ctxForwardingDriverRows) Columns() []string {
	return d.inner.Columns(d.ctx)
}

func (d *ctxForwardingDriverRows) ColumnTypeDatabaseTypeName(index int) string {
	return d.inner.ColumnTypeDatabaseTypeName(d.ctx, index)
}

func (d *ctxForwardingDriverRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return d.inner.ColumnTypeNullable(d.ctx, index)
}

func (d *ctxForwardingDriverRows) HasNextResultSet() bool {
	return d.inner.HasNextResultSet(d.ctx)
}

func (d *ctxForwardingDriverRows) Next(dst []driver.Value) error {
	return d.inner.Next(d.ctx, dst)
}

func (d *ctxForwardingDriverRows) NextResultSet() error {
	return d.inner.NextResultSet(d.ctx)
}

type ctxYieldingResult struct {
	set *ctxYieldOneRowSet
}

func (r *ctxYieldingResult) Close(context.Context) error {
	return nil
}

func (r *ctxYieldingResult) NextResultSet(ctx context.Context) (result.Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return r.set, nil
}

func (*ctxYieldingResult) ResultSets(context.Context) xiter.Seq2[result.Set, error] {
	return func(func(result.Set, error) bool) {
		// unused in these tests
	}
}

type ctxYieldOneRowSet struct {
	emittedRow bool
}

func (*ctxYieldOneRowSet) Index() int { return 0 }

func (*ctxYieldOneRowSet) Columns() []string { return []string{"v"} }

func (*ctxYieldOneRowSet) ColumnTypes() []types.Type { return []types.Type{types.Int64} }

func (s *ctxYieldOneRowSet) Rows(context.Context) xiter.Seq2[result.Row, error] {
	return func(func(result.Row, error) bool) {}
}

func (s *ctxYieldOneRowSet) NextRow(ctx context.Context) (result.Row, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.emittedRow {
		return nil, io.EOF
	}
	s.emittedRow = true

	return scalarInt64Row{}, nil
}

type scalarInt64Row struct{}

func (scalarInt64Row) Values() []value.Value { return nil }

func (scalarInt64Row) Scan(dst ...any) error {
	for _, d := range dst {
		v, ok := d.(*value.Value)
		if ok && v != nil {
			*v = value.Int64Value(42)
		}
	}

	return nil
}

func (scalarInt64Row) ScanNamed(_ ...scanner.NamedDestination) error { return nil }

func (scalarInt64Row) ScanStruct(_ any, _ ...scanner.ScanStructOption) error {
	return nil
}

func TestRows_DriverNextReturnsContextCanceled(t *testing.T) {
	bg := context.Background()

	set := &ctxYieldOneRowSet{}
	res := &ctxYieldingResult{set: set}

	ctx, cancel := context.WithCancel(bg)
	cancel()

	r := &rows{
		conn:   &Conn{ctx: bg},
		result: res,
	}

	err := r.Next(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRows_DriverSecondNextReturnsContextCanceled(t *testing.T) {
	bg := context.Background()

	set := &ctxYieldOneRowSet{}
	res := &ctxYieldingResult{set: set}

	ctx, cancel := context.WithCancel(bg)
	r := &rows{
		conn:   &Conn{ctx: bg},
		result: res,
	}

	dest := []driver.Value{nil}
	err := r.Next(ctx, dest)
	require.NoError(t, err)

	cancel()

	err = r.Next(ctx, dest)
	require.ErrorIs(t, err, context.Canceled)
}

var errStubDriver = errors.New("xsql/rows_context_test: use Connector.Connect")

type sqlStubDriver struct{}

func (sqlStubDriver) Open(string) (driver.Conn, error) {
	return nil, errStubDriver
}

type sqlStubConnector struct{}

func (sqlStubConnector) Driver() driver.Driver {
	return sqlStubDriver{}
}

func (sqlStubConnector) Connect(context.Context) (driver.Conn, error) {
	return &sqlStubConn{}, nil
}

type sqlStubConn struct{}

func (*sqlStubConn) Prepare(string) (driver.Stmt, error) { return nil, errStubDriver }
func (*sqlStubConn) Close() error                        { return nil }
func (*sqlStubConn) Begin() (driver.Tx, error)           { return nil, errStubDriver }

func (*sqlStubConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (
	driver.Rows, error,
) {
	set := &ctxYieldOneRowSet{}
	res := &ctxYieldingResult{set: set}

	inner := &rows{
		conn:   &Conn{ctx: context.Background()},
		result: res,
	}

	return newCtxForwardingDriverRows(ctx, inner), nil
}

func TestDatabaseSQL_RowsErrAfterContextCancelOnSecondNext(t *testing.T) {
	db := sql.OpenDB(sqlStubConnector{})
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())

	sqlRows, err := db.QueryContext(ctx, `SELECT v`)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = sqlRows.Close()
	})

	require.True(t, sqlRows.Next())

	var scanDest int64
	require.NoError(t, sqlRows.Scan(&scanDest))
	require.Equal(t, int64(42), scanDest)

	cancel()

	require.False(t, sqlRows.Next())
	require.ErrorIs(t, sqlRows.Err(), context.Canceled)
}
