package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
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
	if !tx.conn.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	_, err = tx.tx.CommitTx(tx.ctx)
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
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
	if !tx.conn.isReady() {
		return badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	err = tx.tx.Rollback(tx.ctx)
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return err
}

func (tx *tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Rows, err error) {
	onDone := trace.DatabaseSQLOnTxQuery(tx.conn.trace, &ctx, tx.ctx, tx, query, xcontext.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	m := queryModeFromContext(ctx, tx.conn.defaultQueryMode)
	if m != DataQueryMode {
		return nil, badconn.Map(xerrors.WithStackTrace(fmt.Errorf("wrong query mode: %s", m.String())))
	}
	var (
		res    result.Result
		params *table.QueryParameters
	)
	query, params, err = tx.conn.queryParams(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	res, err = tx.tx.Execute(ctx,
		query, params, tx.conn.dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	if err = res.Err(); err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	return &rows{
		conn:   tx.conn,
		result: res,
	}, nil
}

func (tx *tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (_ driver.Result, err error) {
	onDone := trace.DatabaseSQLOnTxExec(tx.conn.trace, &ctx, tx.ctx, tx, query, xcontext.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	m := queryModeFromContext(ctx, tx.conn.defaultQueryMode)
	if m != DataQueryMode {
		return nil, badconn.Map(xerrors.WithStackTrace(fmt.Errorf("wrong query mode: %s", m.String())))
	}
	var params *table.QueryParameters
	query, params, err = tx.conn.queryParams(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	_, err = tx.tx.Execute(ctx,
		query, params, tx.conn.dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	return driver.ResultNoRows, nil
}
