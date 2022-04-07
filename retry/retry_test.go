package retry

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
		err           error               // given error
		backoff       xerrors.BackoffType // no backoff (=== no operationStatus), fast backoff, slow backoff
		deleteSession bool                // close session and delete from pool
		canRetry      CanRetry
	}
	errs := []Case{
		{
			// retryer given unknown error - we will not operationStatus and will close session
			err:           fmt.Errorf("unknown error"),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context deadline exceeded
			err:           context.DeadlineExceeded,
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// golang context canceled
			err:           context.Canceled,
			backoff:       xerrors.BackoffTypeNoBackoff,
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
			err:           xerrors.FromGRPCError(grpc.ErrClientConnClosing),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           xerrors.Transport(),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Canceled),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unknown),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.InvalidArgument),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.DeadlineExceeded),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.NotFound),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.AlreadyExists),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.PermissionDenied),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.ResourceExhausted),
			),
			backoff:       xerrors.BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.FailedPrecondition),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Aborted),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.OutOfRange),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unimplemented),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Internal),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unavailable),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.DataLoss),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Transport(
				xerrors.WithCode(grpcCodes.Unauthenticated),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_REQUEST),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_INTERNAL_ERROR),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_ABORTED),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED),
			),
			backoff:       xerrors.BackoffTypeSlowBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SCHEME_ERROR),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_GENERIC_ERROR),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_TIMEOUT),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_PRECONDITION_FAILED),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_ALREADY_EXISTS),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_NOT_FOUND),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SESSION_EXPIRED),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: true,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNDETERMINED),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_UNSUPPORTED),
			),
			backoff:       xerrors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err: xerrors.Operation(
				xerrors.WithStatusCode(Ydb.StatusIds_SESSION_BUSY),
			),
			backoff:       xerrors.BackoffTypeFastBackoff,
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
