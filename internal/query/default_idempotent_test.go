package query

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	publicQuery "github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDefaultIdempotentOptions(t *testing.T) {
	client := &Client{
		config: config.New(config.WithDefaultIdempotent(true)),
	}

	t.Run("Do uses client default", func(t *testing.T) {
		attempts, err := runIdempotentRetry(client.withDefaultRetryOptions()...)
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
	})

	t.Run("Do per-call false overrides client default", func(t *testing.T) {
		settings := options.ParseDoOpts(nil, publicQuery.WithIdempotent(false))

		attempts, err := runIdempotentRetry(
			client.withDefaultRetryOptions(settings.RetryOpts()...)...,
		)
		require.Error(t, err)
		require.Equal(t, 1, attempts)
	})

	t.Run("Execute uses client default", func(t *testing.T) {
		settings := options.ExecuteSettings(client.withDefaultExecuteOptions()...)

		attempts, err := runIdempotentRetry(settings.RetryOpts()...)
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
	})

	t.Run("Execute per-call false overrides client default", func(t *testing.T) {
		settings := options.ExecuteSettings(client.withDefaultExecuteOptions(
			publicQuery.WithIdempotent(false),
		)...)

		attempts, err := runIdempotentRetry(settings.RetryOpts()...)
		require.Error(t, err)
		require.Equal(t, 1, attempts)
	})
}

func runIdempotentRetry(opts ...retry.Option) (attempts int, err error) {
	err = retry.Retry(context.Background(), func(context.Context) error {
		attempts++
		if attempts == 1 {
			return idempotentRetryableError{}
		}

		return nil
	}, opts...)

	return attempts, err
}

type idempotentRetryableError struct{}

func (idempotentRetryableError) Error() string {
	return "idempotent retryable error"
}

func (idempotentRetryableError) Code() int32 {
	return -1
}

func (idempotentRetryableError) Name() string {
	return "idempotent retryable error"
}

func (idempotentRetryableError) Type() xerrors.Type {
	return xerrors.TypeConditionallyRetryable
}

func (idempotentRetryableError) BackoffType() backoff.Type {
	return backoff.TypeInstant
}
