package table

import (
	"context"
	"fmt"

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
	With(ctx context.Context, f func(ctx context.Context, s *session) error, opts ...retry.Option) error
}

func do(
	ctx context.Context,
	pool sessionPool,
	config *config.Config,
	op table.Operation,
	onAttempt func(err error),
	opts ...retry.Option,
) (err error) {
	fmt.Println("do 1")
	return retryBackoff(ctx, pool,
		func(ctx context.Context, s table.Session) (err error) {
			fmt.Println("do 2")
			defer func() {
				fmt.Println("do defer")
				if onAttempt != nil {
					fmt.Println("do defer onAttempt")
					onAttempt(err)
				}
			}()

			err = func() error {
				fmt.Println("do 3")
				if panicCallback := config.PanicCallback(); panicCallback != nil {
					defer func() {
						fmt.Println("do 3 defer")
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}

				fmt.Println("do 4")
				return op(xcontext.MarkRetryCall(ctx), s)
			}()
			if err != nil {
				fmt.Println("do 5")
				return xerrors.WithStackTrace(err)
			}

			fmt.Println("do 6")
			return nil
		},
		opts...,
	)
}

func retryBackoff(
	ctx context.Context,
	pool sessionPool,
	op table.Operation,
	opts ...retry.Option,
) error {
	fmt.Println("retryBackoff 1")
	return pool.With(ctx, func(ctx context.Context, s *session) error {
		fmt.Println("retryBackoff 2")
		if err := op(ctx, s); err != nil {
			fmt.Println("retryBackoff 3")
			s.checkError(err)

			return xerrors.WithStackTrace(err)
		}

		fmt.Println("retryBackoff 4")
		return nil
	}, opts...)
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
