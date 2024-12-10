package conn

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/session"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type resultNoRows struct{}

func (resultNoRows) LastInsertId() (int64, error) { return 0, ErrUnsupported }
func (resultNoRows) RowsAffected() (int64, error) { return 0, ErrUnsupported }

var _ driver.Result = resultNoRows{}

type Parent interface {
	Query() *query.Client
}

type Conn struct {
	ctx     context.Context //nolint:containedctx
	parent  Parent
	session *query.Session
	onClose []func()
	closed  atomic.Bool
}

func (c *Conn) Exec(ctx context.Context, sql string, params *params.Params) (
	result driver.Result, finalErr error,
) {
	if !c.IsValid() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	if !c.isReady() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	err := c.session.Exec(ctx, sql, options.WithParameters(params))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return resultNoRows{}, nil
}

func (c *Conn) Query(ctx context.Context, sql string, params *params.Params) (
	result driver.RowsNextResultSet, finalErr error,
) {
	if !c.isReady() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	if !c.isReady() {
		return nil, xerrors.WithStackTrace(errNotReadyConn)
	}

	res, err := c.session.Query(ctx, sql,
		options.WithParameters(params),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &rows{
		conn:   c,
		result: res,
	}, nil
}

func (c *Conn) Explain(ctx context.Context, sql string, _ *params.Params) (ast string, plan string, _ error) {
	_, err := c.session.Query(
		ctx, sql,
		options.WithExecMode(options.ExecModeExplain),
		options.WithStatsMode(options.StatsModeNone, func(stats stats.QueryStats) {
			ast = stats.QueryAST()
			plan = stats.QueryPlan()
		}),
	)
	if err != nil {
		return "", "", xerrors.WithStackTrace(err)
	}

	return ast, plan, nil
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

func (c *Conn) isReady() bool {
	return c.session.Status() == session.StatusIdle.String()
}

func (c *Conn) beginTx(ctx context.Context, txOptions driver.TxOptions) (tx iface.Tx, finalErr error) {
	tx, err := beginTx(ctx, c, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return tx, nil
}

func (c *Conn) ID() string {
	return c.session.ID()
}

func (c *Conn) IsValid() bool {
	return c.isReady()
}

func (c *Conn) Ping(ctx context.Context) (finalErr error) {
	if !c.isReady() {
		return xerrors.WithStackTrace(errNotReadyConn)
	}

	if !c.session.Core.IsAlive() {
		return xerrors.WithStackTrace(errNotReadyConn)
	}

	err := c.session.Exec(ctx, "select 1")

	return err
}

func (c *Conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (iface.Tx, error) {
	tx, err := c.beginTx(ctx, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return tx, nil
}

func (c *Conn) Close() (finalErr error) {
	if !c.closed.CompareAndSwap(false, true) {
		return xerrors.WithStackTrace(errConnClosedEarly)
	}

	defer func() {
		for _, onClose := range c.onClose {
			onClose()
		}
	}()

	err := c.session.Close(xcontext.ValueOnly(c.ctx))
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errDeprecated
}
