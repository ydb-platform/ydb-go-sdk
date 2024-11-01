package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// sessionPool is the interface that holds session lifecycle logic.
type sessionPool interface {
	closer.Closer

	Stats() pool.Stats
	With(ctx context.Context, f func(ctx context.Context, s *session) error, preferredNodeId uint32, opts ...retry.Option) error
}

func do(
	ctx context.Context,
	pool sessionPool,
	config *config.Config,
	op table.Operation,
	onAttempt func(err error),
	preferredNodeId uint32,
	opts ...retry.Option,
) (err error) {
	return retryBackoff(ctx, pool,
		func(ctx context.Context, s table.Session) (err error) {
			defer func() {
				if onAttempt != nil {
					onAttempt(err)
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

				return op(xcontext.MarkRetryCall(ctx), s)
			}()
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		preferredNodeId,
		opts...,
	)
}

func retryBackoff(
	ctx context.Context,
	pool sessionPool,
	op table.Operation,
	preferredNodeId uint32,
	opts ...retry.Option,
) error {
	return pool.With(ctx, func(ctx context.Context, s *session) error {
		if err := op(ctx, s); err != nil {
			s.checkError(err)

			return xerrors.WithStackTrace(err)
		}

		return nil
	}, preferredNodeId, opts...)
}

func (c *Client) retryOptions(opts ...table.Option) *table.Options {
	options := &table.Options{
		Trace: c.config.Trace(),
		TxSettings: table.TxSettings(
			table.WithSerializableReadWrite(),
		),
		RetryOptions: []retry.Option{
			retry.WithTrace(c.config.TraceRetry()),
			retry.WithBudget(c.config.RetryBudget()),
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt.ApplyTableOption(options)
		}
	}
	if options.Trace == nil {
		options.Trace = &trace.Table{}
	}

	return options
}
