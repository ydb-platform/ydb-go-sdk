package xsql

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type tx struct {
	conn  *conn
	txCtx context.Context
	tx    table.Transaction
}

var (
	_ driver.Tx                   = &tx{}
	_ driver.ExecerContext        = &tx{}
	_ driver.QueryerContext       = &tx{}
	_ table.TransactionIdentifier = &tx{}
)

func (c *conn) beginTx(ctx context.Context, txOptions driver.TxOptions) (currentTx, error) {
	if c.currentTx != nil {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				fmt.Errorf("broken conn state: conn=%q already have current tx=%q",
					c.ID(), c.currentTx.ID(),
				),
			),
		)
	}
	txc, err := isolation.ToYDB(txOptions)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	transaction, err := c.session.BeginTransaction(ctx, table.TxSettings(txc))
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	c.currentTx = &tx{
		conn:  c,
		txCtx: ctx,
		tx:    transaction,
	}
	return c.currentTx, nil
}

func (tx *tx) ID() string {
	return tx.tx.ID()
}

func (tx *tx) checkTxState() error {
	if tx.conn.currentTx == tx {
		return nil
	}
	if tx.conn.currentTx == nil {
		return fmt.Errorf("broken conn state: tx=%q not related to conn=%q",
			tx.ID(), tx.conn.ID(),
		)
	}
	return fmt.Errorf("broken conn state: tx=%s not related to conn=%q (conn have current tx=%q)",
		tx.conn.currentTx.ID(), tx.conn.ID(), tx.ID(),
	)
}

func (tx *tx) Commit() (finalErr error) {
	onDone := trace.DatabaseSQLOnTxCommit(tx.conn.trace, &tx.txCtx,
		stack.FunctionID(""),
		tx,
	)
	defer func() {
		onDone(finalErr)
	}()
	if err := tx.checkTxState(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	defer func() {
		tx.conn.currentTx = nil
	}()
	_, err := tx.tx.CommitTx(tx.txCtx)
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return nil
}

func (tx *tx) Rollback() (finalErr error) {
	onDone := trace.DatabaseSQLOnTxRollback(tx.conn.trace, &tx.txCtx,
		stack.FunctionID(""),
		tx,
	)
	defer func() {
		onDone(finalErr)
	}()
	if err := tx.checkTxState(); err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	defer func() {
		tx.conn.currentTx = nil
	}()
	err := tx.tx.Rollback(tx.txCtx)
	if err != nil {
		return badconn.Map(xerrors.WithStackTrace(err))
	}
	return err
}

func (tx *tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Rows, finalErr error,
) {
	onDone := trace.DatabaseSQLOnTxQuery(tx.conn.trace, &ctx,
		stack.FunctionID(""),
		tx.txCtx, tx, query, true,
	)
	defer func() {
		onDone(finalErr)
	}()
	m := queryModeFromContext(ctx, tx.conn.defaultQueryMode)
	if m != DataQueryMode {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				xerrors.Retryable(
					fmt.Errorf("wrong query mode: %s", m.String()),
					xerrors.WithDeleteSession(),
					xerrors.WithName("WRONG_QUERY_MODE"),
				),
			),
		)
	}
	query, params, err := tx.conn.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	res, err := tx.tx.Execute(ctx,
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

func (tx *tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (
	_ driver.Result, finalErr error,
) {
	onDone := trace.DatabaseSQLOnTxExec(tx.conn.trace, &ctx,
		stack.FunctionID(""),
		tx.txCtx, tx, query, true,
	)
	defer func() {
		onDone(finalErr)
	}()
	m := queryModeFromContext(ctx, tx.conn.defaultQueryMode)
	if m != DataQueryMode {
		return nil, badconn.Map(
			xerrors.WithStackTrace(
				xerrors.Retryable(
					fmt.Errorf("wrong query mode: %s", m.String()),
					xerrors.WithDeleteSession(),
					xerrors.WithName("WRONG_QUERY_MODE"),
				),
			),
		)
	}
	query, params, err := tx.conn.normalize(query, args...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	_, err = tx.tx.Execute(ctx,
		query, params, tx.conn.dataQueryOptions(ctx)...,
	)
	if err != nil {
		return nil, badconn.Map(xerrors.WithStackTrace(err))
	}
	return resultNoRows{}, nil
}

func (tx *tx) PrepareContext(ctx context.Context, query string) (_ driver.Stmt, finalErr error) {
	onDone := trace.DatabaseSQLOnTxPrepare(tx.conn.trace, &ctx,
		stack.FunctionID(""),
		&tx.txCtx, tx, query,
	)
	defer func() {
		onDone(finalErr)
	}()
	if !tx.conn.isReady() {
		return nil, badconn.Map(xerrors.WithStackTrace(errNotReadyConn))
	}
	return &stmt{
		conn:      tx.conn,
		processor: tx,
		stmtCtx:   ctx,
		query:     query,
		trace:     tx.conn.trace,
	}, nil
}
