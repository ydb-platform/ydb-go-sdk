package retry

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	budget "github.com/ydb-platform/ydb-go-sdk/v3/retry/budget"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type doOptions struct {
	retryOptions []Option
}

// doTxOption defines option for redefine default Retry behavior
type doOption interface {
	ApplyDoOption(opts *doOptions)
}

var (
	_ doOption = doRetryOptionsOption(nil)
	_ doOption = labelOption("")
)

type doRetryOptionsOption []Option

func (retryOptions doRetryOptionsOption) ApplyDoOption(opts *doOptions) {
	opts.retryOptions = append(opts.retryOptions, retryOptions...)
}

// WithDoRetryOptions specified retry options
// Deprecated: use explicit options instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithDoRetryOptions(opts ...Option) doRetryOptionsOption {
	return opts
}

// Do is a retryer of database/sql Conn with fallbacks on errors
func Do(ctx context.Context, db *sql.DB, op func(ctx context.Context, cc *sql.Conn) error, opts ...doOption) error {
	var (
		options = doOptions{
			retryOptions: []Option{
				withCaller(stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/retry.Do")),
			},
		}
		attempts = 0
	)
	if tracer, has := db.Driver().(interface {
		TraceRetry() *trace.Retry
	}); has {
		options.retryOptions = append(options.retryOptions, nil)
		copy(options.retryOptions[1:], options.retryOptions)
		options.retryOptions[0] = WithTrace(tracer.TraceRetry())
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyDoOption(&options)
		}
	}
	err := Retry(ctx, func(ctx context.Context) error {
		attempts++
		cc, err := db.Conn(ctx)
		if err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		defer func() {
			_ = cc.Close()
		}()
		if err = op(xcontext.MarkRetryCall(ctx), cc); err != nil {
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
	retryOptions []Option
}

// doTxOption defines option for redefine default Retry behavior
type doTxOption interface {
	ApplyDoTxOption(o *doTxOptions)
}

var _ doTxOption = doTxRetryOptionsOption(nil)

type doTxRetryOptionsOption []Option

func (doTxRetryOptions doTxRetryOptionsOption) ApplyDoTxOption(o *doTxOptions) {
	o.retryOptions = append(o.retryOptions, doTxRetryOptions...)
}

// WithDoTxRetryOptions specified retry options
// Deprecated: use explicit options instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithDoTxRetryOptions(opts ...Option) doTxRetryOptionsOption {
	return opts
}

var _ doTxOption = txOptionsOption{txOptions: nil}

type txOptionsOption struct {
	txOptions *sql.TxOptions
}

func (txOptions txOptionsOption) ApplyDoTxOption(o *doTxOptions) {
	o.txOptions = txOptions.txOptions
}

// WithTxOptions specified transaction options
func WithTxOptions(txOptions *sql.TxOptions) txOptionsOption {
	return txOptionsOption{
		txOptions: txOptions,
	}
}

// DoTx is a retryer of database/sql transactions with fallbacks on errors
//
//nolint:funlen
func DoTx(ctx context.Context, db *sql.DB, op func(context.Context, *sql.Tx) error, opts ...doTxOption) error {
	var (
		options = doTxOptions{
			retryOptions: []Option{
				withCaller(stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/retry.DoTx")),
			},
			txOptions: &sql.TxOptions{
				Isolation: sql.LevelDefault,
				ReadOnly:  false,
			},
		}
		attempts = 0
	)
	if d, has := db.Driver().(interface {
		TraceRetry() *trace.Retry
		RetryBudget() budget.Budget
	}); has {
		options.retryOptions = append(options.retryOptions, nil, nil)
		copy(options.retryOptions[2:], options.retryOptions)
		options.retryOptions[0] = WithTrace(d.TraceRetry())
		options.retryOptions[1] = WithBudget(d.RetryBudget())
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyDoTxOption(&options)
		}
	}
	err := Retry(ctx, func(ctx context.Context) (finalErr error) {
		attempts++
		tx, err := db.BeginTx(ctx, options.txOptions)
		if err != nil {
			return unwrapErrBadConn(xerrors.WithStackTrace(err))
		}
		defer func() {
			if finalErr == nil {
				return
			}
			errRollback := tx.Rollback()
			if errRollback == nil {
				return
			}
			finalErr = xerrors.NewWithIssues("",
				xerrors.WithStackTrace(finalErr),
				xerrors.WithStackTrace(fmt.Errorf("rollback failed: %w", errRollback)),
			)
		}()
		if err = op(xcontext.MarkRetryCall(ctx), tx); err != nil {
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
