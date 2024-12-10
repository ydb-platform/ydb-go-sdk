package conn

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/connector/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type txFake struct {
	tx.Identifier

	conn *Conn
	ctx  context.Context //nolint:containedctx
}

func (tx *txFake) Exec(ctx context.Context, sql string, params *params.Params) (driver.Result, error) {
	result, err := tx.conn.Exec(ctx, sql, params)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return result, nil
}

func (tx *txFake) Query(ctx context.Context, sql string, params *params.Params) (driver.RowsNextResultSet, error) {
	rows, err := tx.conn.Query(ctx, sql, params)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return rows, nil
}

func beginTxFake(ctx context.Context, c *Conn) iface.Tx {
	return &txFake{
		Identifier: tx.ID("FAKE"),
		conn:       c,
		ctx:        ctx,
	}
}

func (tx *txFake) Commit(ctx context.Context) (err error) {
	if !tx.conn.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	return nil
}

func (tx *txFake) Rollback(ctx context.Context) (err error) {
	if !tx.conn.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}

	return err
}
