package xsql

import (
	"context"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	_ driver.Tx             = &tx{}
	_ driver.QueryerContext = &tx{}
	_ driver.ExecerContext  = &tx{}
)

type tx struct {
	nopResult
	conn        *conn
	transaction table.Transaction
}

var (
	_ driver.Tx             = &tx{}
	_ driver.QueryerContext = &tx{}
	_ driver.ExecerContext  = &tx{}
)

func (tx *tx) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if tx.conn.isClosed() {
		return nil, errClosedConn
	}
	res, err := tx.transaction.Execute(ctx, query, toQueryParams(args))
	if err != nil {
		return nil, tx.conn.checkClosed(err)
	}
	if err = res.Err(); err != nil {
		return nil, tx.conn.checkClosed(res.Err())
	}
	return &rows{
		result: res,
	}, nil
}

func (tx *tx) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if tx.conn.isClosed() {
		return nil, errClosedConn
	}
	res, err := tx.transaction.Execute(ctx, query, toQueryParams(args))
	if err != nil {
		return nil, tx.conn.checkClosed(err)
	}
	if err = res.Err(); err != nil {
		return nil, tx.conn.checkClosed(res.Err())
	}
	return tx, nil
}

func (tx *tx) Commit() (err error) {
	if tx.conn.isClosed() {
		return errClosedConn
	}
	_, err = tx.transaction.CommitTx(context.Background())
	if err != nil {
		return tx.conn.checkClosed(err)
	}
	return nil
}

func (tx *tx) Rollback() (err error) {
	if tx.conn.isClosed() {
		return errClosedConn
	}
	err = tx.transaction.Rollback(context.Background())
	if err != nil {
		return tx.conn.checkClosed(err)
	}
	return err
}
