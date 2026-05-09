package xquery

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/common"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type transaction struct {
	conn *Conn
	tx   query.Transaction
}

func (t *transaction) ID() string {
	return t.tx.ID()
}

func (t *transaction) Exec(ctx context.Context, sql string, params *params.Params) (driver.Result, error) {
	opts := []query.ExecuteOption{
		options.WithParameters(params),
	}

	if txControl := tx.ControlFromContext(ctx, nil); txControl != nil {
		opts = append(opts, options.WithTxControl(txControl))
	}

	if issuesHandler := IssuesHandlerFromContext(ctx); issuesHandler != nil {
		opts = append(opts, options.WithIssuesHandler(issuesHandler.Callback))
	}

	if tx.CommitTxFromContext(ctx) {
		opts = append(opts, options.WithCommit())
	}

	r := &resultWithStats{}
	sm := stats.ModeCallbackFromContextWith(ctx, stats.ModeBasic, r.onQueryStats)
	opts = append(opts, options.WithStatsMode(options.StatsMode(sm.Mode), sm.Callback))

	err := t.tx.Exec(ctx, sql, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return r, nil
}

func (t *transaction) Query(ctx context.Context, sql string, params *params.Params) (common.Rows, error) {
	opts := []query.ExecuteOption{
		options.WithParameters(params),
	}

	if txControl := tx.ControlFromContext(ctx, nil); txControl != nil {
		opts = append(opts, options.WithTxControl(txControl))
	}

	if issuesHandler := IssuesHandlerFromContext(ctx); issuesHandler != nil {
		opts = append(opts, options.WithIssuesHandler(issuesHandler.Callback))
	}

	if tx.CommitTxFromContext(ctx) {
		opts = append(opts, options.WithCommit())
	}

	if sm := stats.ModeCallbackFromContext(ctx); sm != nil {
		opts = append(opts, options.WithStatsMode(options.StatsMode(sm.Mode), sm.Callback))
	}

	result, err := t.tx.Query(ctx, sql, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	rows, err := newRows(ctx, result)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rows, nil
}

func beginTx(ctx context.Context, c *Conn, txOptions driver.TxOptions) (common.Tx, error) {
	txc, err := toYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	nativeTx, err := c.session.Begin(ctx, query.TxSettings(txc))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &transaction{
		conn: c,
		tx:   nativeTx,
	}, nil
}

func (t *transaction) Commit(ctx context.Context) (finalErr error) {
	if err := t.tx.CommitTx(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (t *transaction) Rollback(ctx context.Context) (finalErr error) {
	if err := t.tx.Rollback(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	// Validate connection after rollback RPC - to avoid storing invalid connections in the
	// database/SQL pool after this call. The symmetric commit method does not have this
	// logic, as it needs to inform the upper code about successful commit.
	if !t.conn.IsValid() {
		return badconn.New("session is not valid for reuse after rollback")
	}

	return nil
}
