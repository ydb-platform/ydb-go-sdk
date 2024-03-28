package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestRetryModes(t *testing.T) {
	for _, idempotentType := range []idempotency{
		idempotent,
		nonIdempotent,
	} {
		t.Run(idempotentType.String(), func(t *testing.T) {
			for _, tt := range errsToCheck {
				t.Run(tt.err.Error(), func(t *testing.T) {
					m := Check(tt.err)
					if m.MustRetry(true) != tt.canRetry[idempotent] {
						t.Errorf(
							"unexpected must retry idempotent operation status: %v, want: %v",
							m.MustRetry(true),
							tt.canRetry[idempotent],
						)
					}
					if m.MustRetry(false) != tt.canRetry[nonIdempotent] {
						t.Errorf(
							"unexpected must retry non-idempotent operation status: %v, want: %v",
							m.MustRetry(false),
							tt.canRetry[nonIdempotent],
						)
					}
					if m.BackoffType() != tt.backoff {
						t.Errorf(
							"unexpected backoff status: %v, want: %v",
							m.BackoffType(),
							tt.backoff,
						)
					}
					if m.IsRetryObjectValid() != tt.deleteSession {
						t.Errorf(
							"unexpected delete session status: %v, want: %v",
							m.IsRetryObjectValid(),
							tt.deleteSession,
						)
					}
				})
			}
		})
	}
}

type CustomError struct {
	Err error
}

func (e *CustomError) Error() string {
	return fmt.Sprintf("custom error: %v", e.Err)
}

func (e *CustomError) Unwrap() error {
	return e.Err
}

func TestRetryWithCustomErrors(t *testing.T) {
	var (
		limit = 10
		ctx   = context.Background()
	)
	for _, tt := range []struct {
		error     error
		retriable bool
	}{
		{
			error: &CustomError{
				Err: RetryableError(
					fmt.Errorf("custom error"),
					WithDeleteSession(),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: xerrors.Operation(
					xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
					),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					xerrors.Operation(
						xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
					),
				),
			},
			retriable: false,
		},
	} {
		t.Run(tt.error.Error(), func(t *testing.T) {
			i := 0
			err := Retry(ctx, func(ctx context.Context) error {
				i++
				if i < limit {
					return tt.error
				}

				return nil
			})
			if tt.retriable {
				if i != limit {
					t.Fatalf("unexpected i: %d, queryErr: %v", i, err)
				}
			} else {
				if i != 1 {
					t.Fatalf("unexpected i: %d, queryErr: %v", i, err)
				}
			}
		})
	}
}

func TestRetryTransportDeadlineExceeded(t *testing.T) {
	cancelCounterValue := 5
	for _, code := range []grpcCodes.Code{
		grpcCodes.DeadlineExceeded,
		grpcCodes.Canceled,
	} {
		t.Run(code.String(), func(t *testing.T) {
			counter := 0
			ctx, cancel := xcontext.WithTimeout(context.Background(), time.Hour)
			err := Retry(ctx, func(ctx context.Context) error {
				counter++
				if !(counter < cancelCounterValue) {
					cancel()
				}

				return xerrors.Transport(grpcStatus.Error(code, ""))
			}, WithIdempotent(true))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, cancelCounterValue, counter)
		})
	}
}

func TestRetryTransportCancelled(t *testing.T) {
	cancelCounterValue := 5
	for _, code := range []grpcCodes.Code{
		grpcCodes.DeadlineExceeded,
		grpcCodes.Canceled,
	} {
		t.Run(code.String(), func(t *testing.T) {
			counter := 0
			ctx, cancel := xcontext.WithCancel(context.Background())
			err := Retry(ctx, func(ctx context.Context) error {
				counter++
				if !(counter < cancelCounterValue) {
					cancel()
				}

				return xerrors.Transport(grpcStatus.Error(code, ""))
			}, WithIdempotent(true))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, cancelCounterValue, counter)
		})
	}
}

type MockPanicCallback struct {
	called   bool
	received interface{}
}

func (m *MockPanicCallback) Call(e interface{}) {
	m.called = true
	m.received = e
}

func TestOpWithRecover_NoPanic(t *testing.T) {
	ctx := context.Background()
	options := &retryOptions{
		panicCallback: nil,
	}
	op := func(ctx context.Context) error {
		return nil
	}

	err := opWithRecover(ctx, options, op)

	require.NoError(t, err)
}

func TestOpWithRecover_WithPanic(t *testing.T) {
	ctx := context.Background()
	mockCallback := new(MockPanicCallback)
	options := &retryOptions{
		panicCallback: mockCallback.Call,
	}
	op := func(ctx context.Context) error {
		panic("test panic")
	}

	err := opWithRecover(ctx, options, op)

	require.Error(t, err)
	require.Contains(t, err.Error(), "panic recovered: test panic")
	require.True(t, mockCallback.called)
	require.Equal(t, "test panic", mockCallback.received)
}

func TestHandleContextDone(t *testing.T) {
	attempts := 5
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel to simulate a done context

	err := handleContextDone(ctx, attempts)
	require.Error(t, err)

	expectedMsg := fmt.Sprintf("retry failed on attempt No.%d: %s", attempts, context.Canceled.Error())
	require.Contains(t, err.Error(), expectedMsg)
}

func TestHandleContextError(t *testing.T) {
	attempts := 3
	ctxErr := context.DeadlineExceeded
	opErr := errors.New("operation failed")

	err := handleContextError(attempts, ctxErr, opErr)
	require.Error(t, err)

	expectedMsg := fmt.Sprintf("context error occurred on attempt No.%d", attempts)
	require.Contains(t, err.Error(), expectedMsg)
	require.Contains(t, err.Error(), ctxErr.Error())
	require.Contains(t, err.Error(), opErr.Error())
}

func TestHandleNonRetryableError(t *testing.T) {
	attempts := 2
	idempotent := false
	opErr := errors.New("non-retryable error")

	err := handleNonRetryableError(attempts, idempotent, opErr)
	require.Error(t, err)

	expectedMsg := fmt.Sprintf(
		"non-retryable error occurred on attempt No.%d (idempotent=%v): %s",
		attempts, idempotent, opErr.Error(),
	)
	require.Contains(t, err.Error(), expectedMsg)
}

func TestHandleWaitError(t *testing.T) {
	attempts := 4
	waitErr := errors.New("wait error")
	opErr := errors.New("operation during wait error")

	err := handleWaitError(attempts, waitErr, opErr)
	require.Error(t, err)

	expectedMsg := fmt.Sprintf("wait exit on attempt No.%d", attempts)
	require.Contains(t, err.Error(), expectedMsg)
	require.Contains(t, err.Error(), waitErr.Error())
	require.Contains(t, err.Error(), opErr.Error())
}
