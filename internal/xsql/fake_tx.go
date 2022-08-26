package xsql

// TODO: drop this after implementing it on server-side
//nolint:godox

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fakeTx struct {
	txControl *table.TransactionControl
	conn      *conn
	ctx       context.Context
}

func (tx *fakeTx) ID() string {
	return "fakeTx"
}

func (tx *fakeTx) Commit() (err error) {
	onDone := trace.DatabaseSQLOnTxCommit(tx.conn.trace, &tx.ctx, tx)
	defer func() {
		onDone(err)
	}()
	if tx.conn.isClosed() {
		return errClosedConn
	}
	defer func() {
		tx.conn.currentTx = nil
	}()
	return nil
}

func (tx *fakeTx) Rollback() (err error) {
	onDone := trace.DatabaseSQLOnTxRollback(tx.conn.trace, &tx.ctx, tx)
	defer func() {
		onDone(err)
	}()
	if tx.conn.isClosed() {
		return errClosedConn
	}
	defer func() {
		tx.conn.currentTx = nil
	}()
	return nil
}

func (tx *fakeTx) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (_ driver.Rows, err error) {
	onDone := trace.DatabaseSQLOnTxQuery(tx.conn.trace, &ctx, tx.ctx, tx, q, retry.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	return tx.conn.QueryContext(WithTxControl(ctx, tx.txControl), q, args)
}

func (tx *fakeTx) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (_ driver.Result, err error) {
	onDone := trace.DatabaseSQLOnTxExec(tx.conn.trace, &ctx, tx.ctx, tx, q, retry.IsIdempotent(ctx))
	defer func() {
		onDone(err)
	}()
	return tx.conn.ExecContext(WithTxControl(ctx, tx.txControl), q, args)
}
