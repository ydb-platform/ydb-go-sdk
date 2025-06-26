package retry

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestRetryModes(t *testing.T) {
	for _, idempotentType := range []idempotency{
		idempotent,
		nonIdempotent,
	} {
		t.Run(idempotentType.String(), func(t *testing.T) {
			for i, tt := range errsToCheck {
				t.Run(strconv.Itoa(i)+"."+tt.err.Error(), func(t *testing.T) {
					m := Check(tt.err)
					require.Equal(t, tt.canRetry[idempotent], m.MustRetry(true))
					require.Equal(t, tt.canRetry[nonIdempotent], m.MustRetry(false))
					require.Equal(t, tt.backoff, m.BackoffType())
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
				if counter >= cancelCounterValue {
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
			t.Helper()
			counter := 0
			ctx, cancel := xcontext.WithCancel(context.Background())
			err := Retry(ctx, func(ctx context.Context) error {
				counter++
				if counter >= cancelCounterValue {
					cancel()
				}

				return xerrors.Transport(grpcStatus.Error(code, ""))
			}, WithIdempotent(true))
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, cancelCounterValue, counter)
		})
	}
}

type noQuota struct{}

var errNoQuota = errors.New("no quota")

func (noQuota) Acquire(ctx context.Context) error {
	return errNoQuota
}

func TestRetryWithBudget(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		quota := noQuota{}
		ctx, cancel := context.WithCancel(xtest.Context(t))
		defer cancel()
		err := Retry(ctx, func(ctx context.Context) (err error) {
			return RetryableError(errors.New("custom error"))
		}, WithBudget(quota))
		require.ErrorIs(t, err, errNoQuota)
	})
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
	op := func(ctx context.Context) (*struct{}, error) {
		return nil, nil //nolint:nilnil
	}

	_, err := opWithRecover(ctx, options, op)

	require.NoError(t, err)
}

func TestOpWithRecover_WithPanic(t *testing.T) {
	ctx := context.Background()
	mockCallback := new(MockPanicCallback)
	options := &retryOptions{
		panicCallback: mockCallback.Call,
	}
	op := func(ctx context.Context) (*struct{}, error) {
		panic("test panic")
	}

	_, err := opWithRecover(ctx, options, op)

	require.Error(t, err)
	require.Contains(t, err.Error(), "panic recovered: test panic")
	require.True(t, mockCallback.called)
	require.Equal(t, "test panic", mockCallback.received)
}

func TestRetryWithResult(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		v, err := RetryWithResult(ctx, func(ctx context.Context) (*int, error) {
			v := 123

			return &v, nil
		})
		require.NoError(t, err)
		require.NotNil(t, v)
		require.EqualValues(t, 123, *v)
	})
	t.Run("RetryableError", func(t *testing.T) {
		var counter int
		v, err := RetryWithResult(ctx, func(ctx context.Context) (*int, error) {
			counter++
			if counter < 10 {
				return nil, RetryableError(errors.New("test"))
			}
			v := counter * 123

			return &v, nil
		})
		require.NoError(t, err)
		require.NotNil(t, v)
		require.EqualValues(t, 1230, *v)
		require.EqualValues(t, 10, counter)
	})
	t.Run("Context", func(t *testing.T) {
		t.Run("Cancelled", func(t *testing.T) {
			childCtx, cancel := context.WithCancel(ctx)
			v, err := RetryWithResult(childCtx, func(ctx context.Context) (*int, error) {
				cancel()

				return nil, ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, v)
		})
		t.Run("DeadlineExceeded", func(t *testing.T) {
			childCtx, cancel := context.WithTimeout(ctx, 0)
			v, err := RetryWithResult(childCtx, func(ctx context.Context) (*int, error) {
				cancel()

				return nil, ctx.Err()
			})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, v)
		})
	})
}
