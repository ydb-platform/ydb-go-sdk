package conn

import (
	"context"
	"database/sql/driver"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Parent interface {
		Query() *query.Client
		Trace() *trace.DatabaseSQL
		TraceRetry() *trace.Retry
		RetryBudget() budget.Budget
		Bindings() bind.Bindings
		Clock() clockwork.Clock
	}
	currentTx interface {
		Rollback() error
	}
	Conn struct {
		ctx     context.Context //nolint:containedctx
		parent  Parent
		session *query.Session
		onClose []func()
		closed  atomic.Bool
		currentTx
	}
)

func (c *Conn) ID() string {
	return c.session.ID()
}

func (c *Conn) IsValid() bool {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) CheckNamedValue(value *driver.NamedValue) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) Close() (finalErr error) {
	if !c.closed.CompareAndSwap(false, true) {
		return badconn.Map(xerrors.WithStackTrace(errConnClosedEarly))
	}

	defer func() {
		for _, onClose := range c.onClose {
			onClose()
		}
	}()

	var (
		ctx    = c.ctx
		onDone = trace.DatabaseSQLOnConnClose(
			c.parent.Trace(), &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*Conn).Close"),
		)
	)
	defer func() {
		onDone(finalErr)
	}()
	if c.currentTx != nil {
		_ = c.currentTx.Rollback()
	}
	err := c.session.Close(xcontext.ValueOnly(ctx))
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}

	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Conn) LastUsage() time.Time {
	//TODO implement me
	panic("implement me")
}

func New(ctx context.Context, parent Parent, s *query.Session, opts ...Option) *Conn {
	cc := &Conn{
		ctx:     ctx,
		parent:  parent,
		session: s,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cc)
		}
	}

	return cc
}
