package retry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	budget "github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Do is a retryer of database/sql conn with fallbacks on errors
func Do(ctx context.Context, db *sql.DB, op func(ctx context.Context, cc *sql.Conn) error, opts ...Option) error {
	_, err := DoWithResult(ctx, db, func(ctx context.Context, cc *sql.Conn) (*struct{}, error) {
		err := op(ctx, cc)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return nil, nil //nolint:nilnil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// DoWithResult is a retryer of database/sql conn with fallbacks on errors
func DoWithResult[T any](ctx context.Context, db *sql.DB,
	op func(ctx context.Context, cc *sql.Conn) (T, error),
	opts ...Option,
) (T, error) {
	var (
		zeroValue T
		options   = []Option{
			withCaller(stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/retry.DoWithResult")),
		}
		attempts = 0
	)
	if tracer, has := db.Driver().(interface {
		TraceRetry() *trace.Retry
	}); has {
		options = append(options, nil)
		copy(options[1:], options)
		options[0] = WithTrace(tracer.TraceRetry())
	}

	options = append(options, opts...)

	v, err := RetryWithResult(ctx, func(ctx context.Context) (_ T, finalErr error) {
		attempts++
		cc, err := db.Conn(ctx)
		if err != nil {
			return zeroValue, xerrors.WithStackTrace(err)
		}
		defer func() {
			if finalErr != nil && mustDeleteConn(finalErr, cc) {
				_ = cc.Raw(func(driverConn any) error {
					return xerrors.WithStackTrace(badconn.Errorf("close connection because: %w", finalErr))
				})
			}

			_ = cc.Close()
		}()
		v, err := op(xcontext.MarkRetryCall(ctx), cc)
		if err != nil {
			return zeroValue, xerrors.WithStackTrace(err)
		}

		return v, nil
	}, options...)
	if err != nil {
		return zeroValue, xerrors.WithStackTrace(
			fmt.Errorf("operation failed with %d attempts: %w", attempts, err),
		)
	}

	return v, nil
}

// doTxOption defines option for redefine default Retry behavior
type doTxOption interface {
	ApplyDoTxOption(txOptions *sql.TxOptions)
}

var (
	_ Option     = txOptionsOption{}
	_ doTxOption = txOptionsOption{}
)

type txOptionsOption struct {
	txOptions *sql.TxOptions
}

func (t txOptionsOption) ApplyRetryOption(_ *retryOptions) {
}

func (t txOptionsOption) ApplyDoTxOption(txOptions *sql.TxOptions) {
	if t.txOptions != nil {
		*txOptions = *t.txOptions
	}
}

// WithTxOptions specified transaction options
func WithTxOptions(txOptions *sql.TxOptions) txOptionsOption {
	return txOptionsOption{
		txOptions: txOptions,
	}
}

// DoTx is a retryer of database/sql transactions with fallbacks on errors
func DoTx(ctx context.Context, db *sql.DB, op func(context.Context, *sql.Tx) error, opts ...Option) error {
	_, err := DoTxWithResult(ctx, db, func(ctx context.Context, tx *sql.Tx) (*struct{}, error) {
		err := op(ctx, tx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return nil, nil //nolint:nilnil
	}, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// DoTxWithResult is a retryer of database/sql transactions with fallbacks on errors
func DoTxWithResult[T any](ctx context.Context, db *sql.DB,
	op func(context.Context, *sql.Tx) (T, error),
	opts ...Option,
) (T, error) {
	var (
		zeroValue T
		options   = []Option{
			withCaller(stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/retry.DoTxWithResult")),
		}
		txOptions = &sql.TxOptions{
			Isolation: sql.LevelDefault,
			ReadOnly:  false,
		}
		attempts = 0
	)
	if d, has := db.Driver().(interface {
		TraceRetry() *trace.Retry
		RetryBudget() budget.Budget
	}); has {
		options = append(options, nil, nil)
		copy(options[2:], options)
		options[0] = WithTrace(d.TraceRetry())
		options[1] = WithBudget(d.RetryBudget())
	}
	
	for _, opt := range opts {
		if opt != nil {
			if txOpt, ok := opt.(doTxOption); ok {
				txOpt.ApplyDoTxOption(txOptions)
			} else {
				options = append(options, opt)
			}
		}
	}
	v, err := RetryWithResult(ctx, func(ctx context.Context) (_ T, finalErr error) {
		attempts++
		tx, err := db.BeginTx(ctx, txOptions)
		if err != nil {
			return zeroValue, xerrors.WithStackTrace(err)
		}
		defer func() {
			_ = tx.Rollback()
		}()
		v, err := op(xcontext.MarkRetryCall(ctx), tx)
		if err != nil {
			return zeroValue, xerrors.WithStackTrace(err)
		}
		if err = tx.Commit(); err != nil {
			return zeroValue, xerrors.WithStackTrace(err)
		}

		return v, nil
	}, options...)
	if err != nil {
		return zeroValue, xerrors.WithStackTrace(
			fmt.Errorf("tx operation failed with %d attempts: %w", attempts, err),
		)
	}

	return v, nil
}

func mustDeleteConn[T interface {
	*sql.Conn
}](err error, conn T) bool {
	if xerrors.Is(err, driver.ErrBadConn) {
		return true
	}

	return !xerrors.IsValid(err, conn)
}
