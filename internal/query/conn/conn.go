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
	}
	currentTx interface {
		Rollback() error
	}
	conn struct {
		ctx         context.Context //nolint:containedctx
		parent      Parent
		trace       *trace.DatabaseSQL
		traceRetry  *trace.Retry
		retryBudget budget.Budget
		bindings    []bind.Bind
		session     *query.Session
		clock       clockwork.Clock
		onClose     []func()
		closed      atomic.Bool
		currentTx
	}
)

func (c *conn) ID() string {
	return c.session.ID()
}

func (c *conn) IsValid() bool {
	//TODO implement me
	panic("implement me")
}

func (c *conn) CheckNamedValue(value *driver.NamedValue) error {
	//TODO implement me
	panic("implement me")
}

func (c *conn) Ping(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) Close() (finalErr error) {
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
			c.trace, &ctx,
			stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query/conn.(*conn).Close"),
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

func (c *conn) Begin() (driver.Tx, error) {
	//TODO implement me
	panic("implement me")
}

func (c *conn) LastUsage() time.Time {
	//TODO implement me
	panic("implement me")
}

func New(ctx context.Context, parent Parent, opts ...Option) (*conn, error) {
	s, err := query.CreateSession(ctx, parent.Query())
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	cc := &conn{
		ctx:     ctx,
		parent:  parent,
		session: s,
		clock:   clockwork.NewRealClock(),
		trace:   &trace.DatabaseSQL{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(cc)
		}
	}

	return cc, nil
}
