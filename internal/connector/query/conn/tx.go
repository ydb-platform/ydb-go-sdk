package conn

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector/query/conn/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

type transaction struct {
	conn *Conn
	tx   query.Transaction
}

func (tx *transaction) ID() string {
	return tx.tx.ID()
}

func (tx *transaction) Exec(ctx context.Context, sql string, params *params.Params) (driver.Result, error) {
	err := tx.tx.Exec(ctx, sql, options.WithParameters(params))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return resultNoRows{}, nil
}

func (tx *transaction) Query(ctx context.Context, sql string, params *params.Params) (driver.RowsNextResultSet, error) {
	res, err := tx.tx.Query(ctx,
		sql, options.WithParameters(params),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return &rows{
		conn:   tx.conn,
		result: res,
	}, nil
}

func beginTx(ctx context.Context, c *Conn, txOptions driver.TxOptions) (iface.Tx, error) {
	txc, err := isolation.ToYDB(txOptions)
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

func (tx *transaction) Commit(ctx context.Context) (finalErr error) {
	if err := tx.tx.CommitTx(ctx); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (tx *transaction) Rollback(ctx context.Context) (finalErr error) {
	err := tx.tx.Rollback(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return err
}
