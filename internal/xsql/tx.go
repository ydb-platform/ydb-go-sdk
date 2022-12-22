package xsql

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type tx struct {
	conn *conn
	ctx  context.Context
	tx   table.Transaction
}

var (
	_ driver.Tx                   = &tx{}
	_ driver.ExecerContext        = &tx{}
	_ driver.QueryerContext       = &tx{}
	_ table.TransactionIdentifier = &tx{}
)

func (tx *tx) ID() string {
	return tx.tx.ID()
}

func (tx *tx) Commit() (err error) {
	onDone := trace.DatabaseSQLOnTxCommit(tx.conn.trace, &tx.ctx, tx)
	defer func() {
		onDone(err)
	}()
	defer func() {
		tx.conn.currentTx = nil
	}()
	if tx.conn.isClosed() {
		return errClosedConn
	}
	_, err = tx.tx.CommitTx(tx.ctx)
	if err != nil {
		return tx.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return nil
}

func (tx *tx) Rollback() (err error) {
	onDone := trace.DatabaseSQLOnTxRollback(tx.conn.trace, &tx.ctx, tx)
	defer func() {
		onDone(err)
	}()
	defer func() {
		tx.conn.currentTx = nil
	}()
	if tx.conn.isClosed() {
		return errClosedConn
	}
	err = tx.tx.Rollback(tx.ctx)
	if err != nil {
		return tx.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return err
}

func (tx *tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	onDone := trace.DatabaseSQLOnTxQuery(tx.conn.trace, &ctx, tx.ctx, tx, query, xerrors.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	var res result.Result
	res, err = tx.tx.Execute(ctx,
		query,
		toQueryParams(args),
		dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, tx.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	if err = res.Err(); err != nil {
		return nil, tx.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return &rows{
		conn:   tx.conn,
		result: res,
	}, nil
}

func (tx *tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	onDone := trace.DatabaseSQLOnTxExec(tx.conn.trace, &ctx, tx.ctx, tx, query, xerrors.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	_, err = tx.tx.Execute(ctx,
		query,
		toQueryParams(args),
		dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, tx.conn.checkClosed(xerrors.WithStackTrace(err))
	}
	return driver.ResultNoRows, nil
}
