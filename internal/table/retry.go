package table

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// SessionProvider is the interface that holds session lifecycle logic.
type SessionProvider interface {
	// Get returns alive idle session or creates new one.
	Get(ctx context.Context) (*session, error)

	// Put takes no longer needed session for reuse or deletion depending
	// on implementation.
	// Put must be fast, if necessary must be async
	Put(ctx context.Context, s *session) (err error)
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
