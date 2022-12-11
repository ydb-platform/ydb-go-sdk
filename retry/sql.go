package retry

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type doOptions struct {
	retryOptions []retryOption
}

// doTxOption defines option for redefine default Retry behavior
type doOption func(o *doOptions) error

// WithDoRetryOptions specified retry options
func WithDoRetryOptions(opts ...retryOption) doOption {
	return func(o *doOptions) error {
		o.retryOptions = append(o.retryOptions, opts...)
		return nil
	}
}

// Do is a retryer of database/sql Conn with fallbacks on errors
func Do(ctx context.Context, db *sql.DB, f func(ctx context.Context, cc *sql.Conn) error, opts ...doOption) error {
	var (
		options  = doOptions{}
		attempts = 0
	)
	for _, o := range opts {
		if err := o(&options); err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	err := Retry(ctx, func(ctx context.Context) (err error) {
		attempts++
		cc, err := db.Conn(ctx)
		if err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		if err = f(ctx, cc); err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		return nil
	}, options.retryOptions...)
	if err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("operation failed with %d attempts: %w", attempts, err),
		)
	}
	return nil
}

type doTxOptions struct {
	txOptions    *sql.TxOptions
	retryOptions []retryOption
}

// doTxOption defines option for redefine default Retry behavior
type doTxOption func(o *doTxOptions) error

// WithDoTxRetryOptions specified retry options
func WithDoTxRetryOptions(opts ...retryOption) doTxOption {
	return func(o *doTxOptions) error {
		o.retryOptions = append(o.retryOptions, opts...)
		return nil
	}
}

// WithTxOptions specified transaction options
func WithTxOptions(txOptions *sql.TxOptions) doTxOption {
	return func(o *doTxOptions) error {
		o.txOptions = txOptions
		return nil
	}
}

// DoTx is a retryer of database/sql transactions with fallbacks on errors
func DoTx(ctx context.Context, db *sql.DB, f func(context.Context, *sql.Tx) error, opts ...doTxOption) error {
	var (
		options = doTxOptions{
			txOptions: &sql.TxOptions{
				Isolation: sql.LevelDefault,
				ReadOnly:  false,
			},
		}
		attempts = 0
	)
	for _, o := range opts {
		if err := o(&options); err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	err := Retry(ctx, func(ctx context.Context) (err error) {
		attempts++
		tx, err := db.BeginTx(ctx, options.txOptions)
		if err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = tx.Rollback()
		}()
		if err = f(ctx, tx); err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		if err = tx.Commit(); err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		return nil
	}, options.retryOptions...)
	if err != nil {
		return xerrors.WithStackTrace(
			fmt.Errorf("tx operation failed with %d attempts: %w", attempts, err),
		)
	}
	return nil
}
