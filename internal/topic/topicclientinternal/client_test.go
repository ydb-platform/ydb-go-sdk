package topicclientinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

func TestStartTransactionalWriterContextMaterializationError(t *testing.T) {
	for _, multiWriter := range []bool{false, true} {
		opts := []topicoptions.WriterOption{}
		if multiWriter {
			opts = append(opts, topicoptions.WithWriteToManyPartitions())
		}

		for _, test := range []struct {
			name   string
			cancel bool
			err    error
		}{
			{name: "BeginFailed", err: errors.New("begin failed")},
			{name: "Cancelled", cancel: true, err: context.Canceled},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if test.cancel {
					cancel()
				}

				calls := 0
				transaction := &materializingTransaction{
					unlazy: func(got context.Context) error {
						calls++
						require.Same(t, ctx, got)
						if test.cancel {
							return got.Err()
						}

						return test.err
					},
				}

				// No topic client is configured: a failed Begin must return before starting any writer.
				client := &Client{}
				writer, err := client.StartTransactionalWriterContext(ctx, transaction, "topic", opts...)
				require.ErrorIs(t, err, test.err)
				require.Nil(t, writer)
				require.Equal(t, 1, calls)
			})
		}
	}
}

func TestStartTransactionalWriterContextUnsupportedTransaction(t *testing.T) {
	client := &Client{}
	writer, err := client.StartTransactionalWriterContext(t.Context(), tx.ID("transaction-id"), "topic")
	require.ErrorIs(t, err, errUnsupportedTransactionType)
	require.Nil(t, writer)
}

type materializingTransaction struct {
	tx.Transaction

	unlazy func(context.Context) error
}

func (tx *materializingTransaction) UnLazy(ctx context.Context) error {
	return tx.unlazy(ctx)
}
