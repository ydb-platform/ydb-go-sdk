package xsql

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	internalxsql "github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type connOption func(*conn)

type currentTx interface {
	tx.Identifier
	driver.Tx
	driver.ExecerContext
	driver.QueryerContext
	driver.ConnPrepareContext
}

type beginTxFunc func(ctx context.Context, txOptions driver.TxOptions) (currentTx, error)

type conn struct {
	ctx context.Context

	connector *internalxsql.Connector
	trace     *trace.DatabaseSQL
	session   query.ClosableSession

	beginTxFuncs map[internalxsql.QueryMode]beginTxFunc

	closed           atomic.Bool
	lastUsage        atomic.Int64
	defaultQueryMode internalxsql.QueryMode

	defaultTxControl *query.TransactionControl
	dataOpts         []options.ExecuteDataQueryOption

	scanOpts []options.ExecuteScanQueryOption

	currentTx currentTx
}

var (
	_ driver.Conn               = &conn{}
	_ driver.ConnPrepareContext = &conn{}
	_ driver.ConnBeginTx        = &conn{}
	_ driver.ExecerContext      = &conn{}
	_ driver.QueryerContext     = &conn{}
	_ driver.Pinger             = &conn{}
	_ driver.Validator          = &conn{}
	_ driver.NamedValueChecker  = &conn{}
)

type resultNoRows struct{}

func (resultNoRows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (resultNoRows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

var (
	_ driver.Result = resultNoRows{}
)

func newConn(ctx context.Context, c *internalxsql.Connector, opts ...connOption) *conn {
	panic("unimplemented")
}

// CheckNamedValue implements driver.NamedValueChecker.
func (c *conn) CheckNamedValue(*driver.NamedValue) error {
	panic("unimplemented")
}

// IsValid implements driver.Validator.
func (c *conn) IsValid() bool {
	panic("unimplemented")
}

// Ping implements driver.Pinger.
func (c *conn) Ping(ctx context.Context) error {
	panic("unimplemented")
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	panic("unimplemented")
}

// ExecContext implements driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	panic("unimplemented")
}

// BeginTx implements driver.ConnBeginTx.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	panic("unimplemented")
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	panic("unimplemented")
}

// Begin implements driver.Conn.
func (c *conn) Begin() (driver.Tx, error) {
	panic("unimplemented")
}

// Close implements driver.Conn.
func (c *conn) Close() error {
	panic("unimplemented")
}

// Prepare implements driver.Conn.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	panic("unimplemented")
}
