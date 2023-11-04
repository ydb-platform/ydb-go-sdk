package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
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

func doTx(
	ctx context.Context,
	c SessionProvider,
	config *config.Config,
	op table.TxOperation,
	opts *table.Options,
) (err error) {
	if opts.Trace == nil {
		opts.Trace = &trace.Table{}
	}
	attempts, onIntermediate := 0, trace.TableOnDoTx(opts.Trace, &ctx, trace.FunctionID(1),
		opts.Label, opts.Label, opts.Idempotent, xcontext.IsNestedCall(ctx),
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	return retryBackoff(ctx, c,
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
					errRollback := tx.Rollback(ctx)
					if errRollback != nil {
						err = xerrors.NewWithIssues("",
							xerrors.WithStackTrace(err),
							xerrors.WithStackTrace(errRollback),
						)
					} else {
						err = xerrors.WithStackTrace(err)
					}
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
				return op(xcontext.MarkRetryCall(ctx), tx)
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
		opts.RetryOptions...,
	)
}

func do(
	ctx context.Context,
	c SessionProvider,
	config *config.Config,
	op table.Operation,
	opts *table.Options,
) (err error) {
	if opts.Trace == nil {
		opts.Trace = &trace.Table{}
	}
	attempts, onIntermediate := 0, trace.TableOnDo(opts.Trace, &ctx, trace.FunctionID(1),
		opts.Label, opts.Label, opts.Idempotent, xcontext.IsNestedCall(ctx),
	)
	defer func() {
		onIntermediate(err)(attempts, err)
	}()
	return retryBackoff(ctx, c,
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
				return op(xcontext.MarkRetryCall(ctx), s)
			}()

			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		opts.RetryOptions...,
	)
}

func retryBackoff(
	ctx context.Context,
	p SessionProvider,
	op table.Operation,
	opts ...retry.Option,
) error {
	return retry.Retry(ctx,
		func(ctx context.Context) (err error) {
			var s *session

			s, err = p.Get(ctx)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			defer func() {
				_ = p.Put(ctx, s)
			}()

			if err = op(ctx, s); err != nil {
				s.checkError(err)
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		opts...,
	)
}

func (c *Client) retryOptions(opts ...table.Option) *table.Options {
	options := &table.Options{
		Trace: c.config.Trace(),
		TxSettings: table.TxSettings(
			table.WithSerializableReadWrite(),
		),
		RetryOptions: []retry.Option{
			retry.WithTrace(c.config.TraceRetry()),
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTableOption(options)
		}
	}
	return options
}
