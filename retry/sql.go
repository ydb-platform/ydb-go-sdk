package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/isolation"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// TxOperationFunc is a user-defined lambda for retrying
type TxOperationFunc func(context.Context, *sql.Tx) error

type doTxOptions struct {
	txOptions  *sql.TxOptions
	idempotent bool
}

// DoTxOption defines option for redefine default DoTx behavior
type DoTxOption func(o *doTxOptions) error

// Idempotent marked TxOperation as idempotent for best effort retrying
func Idempotent(idempotent bool) DoTxOption {
	return func(o *doTxOptions) error {
		o.idempotent = idempotent
		return nil
	}
}

// WithTxOptions replaces default txOptions
func WithTxOptions(txOptions *sql.TxOptions) DoTxOption {
	return func(o *doTxOptions) error {
		o.txOptions = txOptions
		return nil
	}
}

// WithTxSettings makes driver.TxOptions by given txControl
func WithTxSettings(txControl *table.TransactionSettings) DoTxOption {
	return func(o *doTxOptions) error {
		txOptions, err := isolation.FromYDB(txControl)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		o.txOptions = txOptions
		return nil
	}
}

// DoTx is a shortcut for calling Do(ctx, f) on initialized TxDoer with DB field set to given db.
func DoTx(ctx context.Context, db *sql.DB, f TxOperationFunc, opts ...DoTxOption) error {
	options := doTxOptions{
		txOptions: &sql.TxOptions{
			Isolation: sql.LevelDefault,
			ReadOnly:  false,
		},
		idempotent: false,
	}
	for _, o := range opts {
		if err := o(&options); err != nil {
			return xerrors.WithStackTrace(err)
		}
	}
	err := Retry(ctx, func(ctx context.Context) (err error) {
		tx, err := db.BeginTx(ctx, options.txOptions)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = tx.Rollback()
		}()
		if err = f(ctx, tx); err != nil {
			switch {
			case xerrors.Is(err, driver.ErrBadConn):
				return xerrors.WithStackTrace(xerrors.Retryable(err, xerrors.WithDeleteSession()))
			default:
				return xerrors.WithStackTrace(err)
			}
		}
		if err = tx.Commit(); err != nil {
			return xerrors.WithStackTrace(err)
		}
		return nil
	}, WithIdempotent(options.idempotent))
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}
