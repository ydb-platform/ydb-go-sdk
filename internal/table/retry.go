package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(context.Context) (*session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(context.Context, *session) (err error)
}

type (
	markRetryCallKey struct{}
)

func markRetryCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, markRetryCallKey{}, true)
}

func isRetryCalledAbove(ctx context.Context) bool {
	if _, has := ctx.Value(markRetryCallKey{}).(bool); has {
		return true
	}
	return false
}

func doTx(
	ctx context.Context,
	c SessionProvider,
	config config.Config,
	op table.TxOperation,
	opts table.Options,
) (err error) {
	attempts, onIntermediate := 0, trace.TableOnDoTx(
		opts.Trace,
		&ctx,
		opts.Idempotent,
		isRetryCalledAbove(ctx),
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	err = retryBackoff(
		ctx,
		c,
		opts.FastBackoff,
		opts.SlowBackoff,
		opts.Idempotent,
		func(ctx context.Context, s table.Session) (err error) {
			attempts++

			defer func() {
				onIntermediate(err)
			}()

			tx, err := s.BeginTransaction(ctx, opts.TxSettings)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			defer func() {
				if err != nil {
					_ = tx.Rollback(ctx)
				}
			}()

			err = func() error {
				if panicCallback := config.PanicCallback(); panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}
				return op(ctx, tx)
			}()

			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			_, err = tx.CommitTx(ctx, opts.TxCommitOptions...)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func do(
	ctx context.Context,
	c SessionProvider,
	config config.Config,
	op table.Operation,
	opts table.Options,
) (err error) {
	attempts, onIntermediate := 0, trace.TableOnDo(opts.Trace, &ctx, opts.Idempotent, isRetryCalledAbove(ctx))
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	return retryBackoff(ctx, c, opts.FastBackoff, opts.SlowBackoff, opts.Idempotent,
		func(ctx context.Context, s table.Session) (err error) {
			attempts++

			defer func() {
				onIntermediate(err)
			}()

			err = func() error {
				if panicCallback := config.PanicCallback(); panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}
				return op(ctx, s)
			}()

			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
	)
}

func retryBackoff(
	ctx context.Context,
	p SessionProvider,
	fastBackoff backoff.Backoff,
	slowBackoff backoff.Backoff,
	isOperationIdempotent bool,
	op table.Operation,
) (err error) {
	err = retry.Retry(markRetryCall(ctx),
		func(ctx context.Context) (err error) {
			var s *session

			s, err = p.Get(ctx)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			defer func() {
				_ = p.Put(ctx, s)
			}()

			err = op(ctx, s)
			if err != nil {
				s.checkError(err)
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		retry.WithFastBackoff(fastBackoff),
		retry.WithSlowBackoff(slowBackoff),
		retry.WithIdempotent(isOperationIdempotent),
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func retryOptions(trace trace.Table, opts ...table.Option) table.Options {
	options := table.Options{
		Trace:       trace,
		FastBackoff: backoff.Fast,
		SlowBackoff: backoff.Slow,
		TxSettings: table.TxSettings(
			table.WithSerializableReadWrite(),
		),
	}
	for _, o := range opts {
		if o != nil {
			o(&options)
		}
	}
	return options
}
