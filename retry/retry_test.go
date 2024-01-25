package retry

import (
	"context"
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
					if m.MustDeleteSession() != tt.deleteSession {
						t.Errorf(
							"unexpected delete session status: %v, want: %v",
							m.MustDeleteSession(),
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
	}
}

func TestRetryTransportCancelled(t *testing.T) {
	cancelCounterValue := 5
	for _, code := range []grpcCodes.Code{
		grpcCodes.DeadlineExceeded,
		grpcCodes.Canceled,
	} {
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
	}
}
