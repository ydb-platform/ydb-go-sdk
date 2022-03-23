package retry

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

func TestLogBackoff(t *testing.T) {
	type exp struct {
		eq  time.Duration
		gte time.Duration
		lte time.Duration
	}
	for _, test := range []struct {
		name    string
		backoff logBackoff
		exp     []exp
		seeds   int64
	}{
		{
			backoff: newBackoff(
				withSlotDuration(time.Second),
				withCeiling(3),
				withJitterLimit(0),
			),
			exp: []exp{
				{gte: 0, lte: time.Second},     // 1 << min(0, 3)
				{gte: 0, lte: 2 * time.Second}, // 1 << min(1, 3)
				{gte: 0, lte: 4 * time.Second}, // 1 << min(2, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(3, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(4, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(5, 3)
				{gte: 0, lte: 8 * time.Second}, // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: newBackoff(
				withSlotDuration(time.Second),
				withCeiling(3),
				withJitterLimit(0.5),
			),
			exp: []exp{
				{gte: 500 * time.Millisecond, lte: time.Second}, // 1 << min(0, 3)
				{gte: 1 * time.Second, lte: 2 * time.Second},    // 1 << min(1, 3)
				{gte: 2 * time.Second, lte: 4 * time.Second},    // 1 << min(2, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(3, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(4, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(5, 3)
				{gte: 4 * time.Second, lte: 8 * time.Second},    // 1 << min(6, 3)
			},
			seeds: 1000,
		},
		{
			backoff: newBackoff(
				withSlotDuration(time.Second),
				withCeiling(3),
				withJitterLimit(1),
			),
			exp: []exp{
				{eq: time.Second},     // 1 << min(0, 3)
				{eq: 2 * time.Second}, // 1 << min(1, 3)
				{eq: 4 * time.Second}, // 1 << min(2, 3)
				{eq: 8 * time.Second}, // 1 << min(3, 3)
				{eq: 8 * time.Second}, // 1 << min(4, 3)
				{eq: 8 * time.Second}, // 1 << min(5, 3)
				{eq: 8 * time.Second}, // 1 << min(6, 3)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.seeds == 0 {
				test.seeds = 1
			}
			for seed := int64(0); seed < test.seeds; seed++ {
				// Fix random to reproduce the tests.
				rand.Seed(seed)

				for n, exp := range test.exp {
					act := test.backoff.delay(n)
					if exp := exp.eq; exp != 0 {
						if exp != act {
							t.Fatalf(
								"unexpected Backoff delay: %s; want %s",
								act, exp,
							)
						}
						continue
					}
					if gte := exp.gte; act < gte {
						t.Errorf(
							"unexpected Backoff delay: %s; want >= %s",
							act, gte,
						)
					}
					if lte := exp.lte; act > lte {
						t.Errorf(
							"unexpected Backoff delay: %s; want <= %s",
							act, lte,
						)
					}
				}
			}
		})
	}
}

func TestRetryModes(t *testing.T) {
	type CanRetry struct {
		idempotentOperation    bool // after an error we must retry idempotent operation or no
		nonIdempotentOperation bool // after an error we must retry non-idempotent operation or no
	}
	type Case struct {
		err           error              // given error
		backoff       errors.BackoffType // no backoff (=== no operationStatus), fast backoff, slow backoff
		deleteSession bool               // close session and delete from pool
		canRetry      CanRetry
	}
	errs := []Case{
		{
			// retryer given unknown error - we will not operationStatus and will close session
			err:           fmt.Errorf("unknown error"),
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context deadline exceeded
			err:           context.DeadlineExceeded,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context canceled
			err:           context.Canceled,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// nolint:staticcheck
			// ignore SA1019
			// We want to check internal grpc error on chaos monkey testing
			// nolint:nolintlint
			err:           errors.FromGRPCError(grpc.ErrClientConnClosing),
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnknownCode,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorCanceled,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnknown,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorInvalidArgument,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorDeadlineExceeded,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorNotFound,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorAlreadyExists,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorPermissionDenied,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorResourceExhausted,
			},
			backoff:       errors.BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorFailedPrecondition,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorAborted,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorOutOfRange,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnimplemented,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorInternal,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnavailable,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorDataLoss,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnauthenticated,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusUnknownStatus,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusBadRequest,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusUnauthorized,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusInternalError,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusAborted,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusUnavailable,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusOverloaded,
			},
			backoff:       errors.BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusSchemeError,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusGenericError,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusTimeout,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusBadSession,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusPreconditionFailed,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusAlreadyExists,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusNotFound,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusSessionExpired,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusCancelled,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusUndetermined,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusUnsupported,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: &errors.OperationError{
				Reason: errors.StatusSessionBusy,
			},
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
	}
	for _, test := range errs {
		t.Run(test.err.Error(), func(t *testing.T) {
			m := Check(test.err)
			if m.MustRetry(true) != test.canRetry.idempotentOperation {
				t.Errorf(
					"unexpected must retry idempotent operation status: %v, want: %v",
					m.MustRetry(true),
					test.canRetry.idempotentOperation,
				)
			}
			if m.MustRetry(false) != test.canRetry.nonIdempotentOperation {
				t.Errorf(
					"unexpected must retry non-idempotent operation status: %v, want: %v",
					m.MustRetry(false),
					test.canRetry.nonIdempotentOperation,
				)
			}
			if m.backoff != test.backoff {
				t.Errorf(
					"unexpected backoff status: %v, want: %v",
					m.backoff,
					test.backoff,
				)
			}
			if m.deleteSession != test.deleteSession {
				t.Errorf(
					"unexpected delete session status: %v, want: %v",
					m.deleteSession,
					test.deleteSession,
				)
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
	for _, test := range []struct {
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
				Err: errors.NewOpError(
					errors.WithOEReason(
						errors.StatusBadSession,
					),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					errors.NewOpError(
						errors.WithOEReason(
							errors.StatusBadSession,
						),
					),
				),
			},
			retriable: true,
		},
		{
			error: &CustomError{
				Err: fmt.Errorf(
					"wrapped error: %w",
					errors.NewOpError(
						errors.WithOEReason(
							errors.StatusUnauthorized,
						),
					),
				),
			},
			retriable: false,
		},
	} {
		t.Run(test.error.Error(), func(t *testing.T) {
			i := 0
			err := Retry(ctx, func(ctx context.Context) (err error) {
				i++
				if i < limit {
					return test.error
				}
				return nil
			})
			if test.retriable {
				if i != limit {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
			} else {
				if i != 1 {
					t.Fatalf("unexpected i: %d, err: %v", i, err)
				}
			}
		})
	}
}
