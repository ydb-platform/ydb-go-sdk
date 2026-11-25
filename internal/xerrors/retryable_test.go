package xerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
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
		require.NoError(t, RetryableError(Unretryable(Retryable(errors.New("test")))))
	})
}

func TestUnretryableUnwrap(t *testing.T) {
	test := errors.New("test")
	wrapped := Unretryable(test)
	require.ErrorIs(t, wrapped, test)
}

func TestUnretryableCheck(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		code int32
	}{
		{
			name: "operation",
			err:  Operation(WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED)),
			code: int32(Ydb.StatusIds_SESSION_EXPIRED),
		},
		{
			name: "transport",
			err:  Transport(grpcStatus.Error(grpcCodes.Unavailable, "unavailable")),
			code: int32(grpcCodes.Unavailable),
		},
		{
			name: "custom retryable",
			err:  Retryable(errors.New("retryable")),
			code: -1,
		},
		{
			name: "ordinary error",
			err:  errors.New("ordinary"),
			code: -1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := fmt.Errorf("outer: %w", Unretryable(fmt.Errorf("inner: %w", tt.err)))
			code, errType, backoffType := Check(err)
			require.Equal(t, TypeNonRetryable, errType)
			require.EqualValues(t, tt.code, code)
			require.Equal(t, backoff.TypeInstant, backoffType)
			require.ErrorIs(t, err, tt.err)
			require.EqualError(t, err, "outer: inner: "+tt.err.Error())
			var classified Error
			require.ErrorAs(t, err, &classified)
			require.Contains(t, classified.Name(), "unretryable/")
		})
	}
}

func TestIsValid(t *testing.T) {
	type myType struct{}
	obj := &myType{}
	err := Retryable(fmt.Errorf("test"), Invalid(obj))
	require.False(t, IsValid(err, obj))
	require.True(t, IsValid(err, &myType{}))
	var objAsInterface any
	objAsInterface = obj
	require.False(t, IsValid(err, objAsInterface))
	objAsInterface = &myType{}
	require.True(t, IsValid(err, objAsInterface))
}
