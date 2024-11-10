package conn

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session"
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
		currentTx
		ctx       context.Context //nolint:containedctx
		parent    Parent
		session   *query.Session
		onClose   []func()
		closed    atomic.Bool
		lastUsage atomic.Int64
	}
)

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

func (c *Conn) isReady() bool {
	return c.session.Status() == session.StatusIdle.String()
}

func (c *Conn) execContext(
	ctx context.Context,
	query string,
	args []driver.NamedValue,
) (_ driver.Result, finalErr error) {
	defer func() {
		c.lastUsage.Store(c.parent.Clock().Now().Unix())
	}()

	if !c.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	// TODO tx
	// if c.currentTx != nil {
	// 	return c.currentTx.ExecContext(ctx, query, args)
	// }

	m := queryModeFromContext(ctx, c.defaultQueryMode)
	onDone := trace.DatabaseSQLOnConnExec(c.parent.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn.(*Conn).execContext"),
		query, m.String(), xcontext.IsIdempotent(ctx), c.parent.Clock().Since(c.LastUsage()),
	)
	defer func() {
		onDone(finalErr)
	}()

	c.session.Exec()
	// switch m {
	// case DataQueryMode:
	// 	return c.executeDataQuery(ctx, query, args)
	// case SchemeQueryMode:
	// 	return c.executeSchemeQuery(ctx, query)
	// case ScriptingQueryMode:
	// 	return c.executeScriptingQuery(ctx, query, args)
	// default:
	// 	return nil, fmt.Errorf("unsupported query mode '%s' for execute query", m)
	// }
}
