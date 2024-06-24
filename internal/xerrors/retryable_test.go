package xerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
)

func TestRetryableCode(t *testing.T) {
	for _, tt := range []struct {
		err  error
		code int32
	}{
		{
			err:  Retryable(fmt.Errorf("some")),
			code: -1,
		},
		{
			err: Retryable(
				Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			),
			code: int32(grpcCodes.Unavailable),
		},
		{
			err: Retryable(
				Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			),
			code: int32(Ydb.StatusIds_BAD_REQUEST),
		},
		{
			err: Retryable(Retryable(
				Transport(grpcStatus.Error(grpcCodes.Unavailable, "")),
			)),
			code: int32(grpcCodes.Unavailable),
		},
		{
			err: Retryable(Retryable(
				Operation(WithStatusCode(Ydb.StatusIds_BAD_REQUEST)),
			)),
			code: int32(Ydb.StatusIds_BAD_REQUEST),
		},
	} {
		t.Run("", func(t *testing.T) {
			var err Error
			require.ErrorAs(t, tt.err, &err)
			require.Equal(t, tt.code, err.Code())
		})
	}
}

func TestRetriableError(t *testing.T) {
	t.Run("retryable", func(t *testing.T) {
		retriable := Retryable(errors.New("test"))
		wrapped := fmt.Errorf("wrap: %w", retriable)
		require.Equal(t, retriable, RetryableError(retriable))
		require.Equal(t, retriable, RetryableError(wrapped))
	})
	t.Run("unretryable", func(t *testing.T) {
		require.NoError(t, RetryableError(errors.New("test")))
		require.NoError(t, RetryableError(Nonretryable(Retryable(errors.New("test")))))
	})
}
