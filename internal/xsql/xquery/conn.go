package xquery

import (
	"context"
	"database/sql/driver"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
)

type Parent interface {
	Query() *query.Client
}

type Conn struct {
	ctx     context.Context //nolint:containedctx
	session *query.Session
	onClose []func()
	closed  atomic.Bool
	fakeTx  bool
}

func (c *Conn) NodeID() uint32 {
	return c.session.NodeID()
}

func toQueryStatsMode(mode stats.Mode) options.StatsMode {
	switch mode {
	case stats.ModeBasic:
		return options.StatsModeBasic
	case stats.ModeFull:
		return options.StatsModeFull
	case stats.ModeProfile:
		return options.StatsModeProfile
	default:
		return options.StatsModeNone
	}
}

func (c *Conn) Exec(ctx context.Context, sql string, params *params.Params) (
	driver.Result, error,
) {
	if !c.IsValid() {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
	}

	if !c.isReady() {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
	}

	opts := []options.Execute{
		options.WithParameters(params),
	}

	if txControl := tx.ControlFromContext(ctx, nil); txControl != nil {
		opts = append(opts, options.WithTxControl(txControl))
	}

	r := &resultWithStats{}
	statsMode := options.StatsModeBasic
	onStats := r.onQueryStats
	if sm := stats.ModeCallbackFromContext(ctx); sm != nil {
		statsMode = toQueryStatsMode(sm.Mode)
		onStats = func(qs stats.QueryStats) {
			r.onQueryStats(qs)
			sm.Callback(qs)
		}
	}
	opts = append(opts, options.WithStatsMode(statsMode, onStats))

	err := c.session.Exec(ctx, sql, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (c *Conn) Query(ctx context.Context, sql string, params *params.Params) (
	result driver.RowsNextResultSet, finalErr error,
) {
	if !c.isReady() {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
	}

	opts := []options.Execute{
		options.WithParameters(params),
	}

	if txControl := tx.ControlFromContext(ctx, nil); txControl != nil {
		opts = append(opts, options.WithTxControl(txControl))
	}

	if sm := stats.ModeCallbackFromContext(ctx); sm != nil {
		opts = append(opts, options.WithStatsMode(toQueryStatsMode(sm.Mode), sm.Callback))
	}

	res, err := c.session.Query(ctx, sql, opts...)
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

func New(ctx context.Context, s *query.Session, opts ...Option) *Conn {
	cc := &Conn{
		ctx:     ctx,
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
	return c.session.Status() == query.StatusIdle.String()
}

func (c *Conn) beginTx(ctx context.Context, txOptions driver.TxOptions) (tx common.Tx, finalErr error) {
	if c.fakeTx {
		return beginTxFake(ctx, c), nil
	}

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
		return xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
	}

	if !c.session.IsAlive() {
		return xerrors.WithStackTrace(xerrors.Retryable(errNotReadyConn,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
	}

	err := c.session.Exec(ctx, "select 1")

	return err
}

func (c *Conn) BeginTx(ctx context.Context, txOptions driver.TxOptions) (common.Tx, error) {
	tx, err := c.beginTx(ctx, txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return tx, nil
}

func (c *Conn) Close() (finalErr error) {
	if !c.closed.CompareAndSwap(false, true) {
		return xerrors.WithStackTrace(xerrors.Retryable(errConnClosedEarly,
			xerrors.Invalid(c),
			xerrors.Invalid(c.session),
		))
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
