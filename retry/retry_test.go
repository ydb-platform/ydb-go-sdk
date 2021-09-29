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
			backoff: logBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0,
			},
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
			backoff: logBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  0.5,
			},
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
			backoff: logBackoff{
				SlotDuration: time.Second,
				Ceiling:      3,
				JitterLimit:  1,
			},
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
		backoff       errors.BackoffType // type of backoff: no backoff (=== no operationCompleted), fast backoff, slow backoff
		deleteSession bool               // close session and delete from pool
		canRetry      CanRetry
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not operationCompleted and will close session
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			err:           context.Canceled, // golang context canceled
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    false,
				nonIdempotentOperation: false,
			},
		},
		{
			// nolint:staticcheck
			err:           errors.MapGRPCError(grpc.ErrClientConnClosing),
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
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
			deleteSession: false,
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
				Reason: errors.StatusNotFound,
			},
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
			canRetry: CanRetry{
				idempotentOperation:    true,
				nonIdempotentOperation: true,
			},
		},
		{
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
			err: &errors.OpError{
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
				t.Errorf("unexpected must retry idempotent operation status: %v, want: %v", m.MustRetry(true), test.canRetry.idempotentOperation)
			}
			if m.MustRetry(false) != test.canRetry.nonIdempotentOperation {
				t.Errorf("unexpected must retry non-idempotent operation status: %v, want: %v", m.MustRetry(false), test.canRetry.nonIdempotentOperation)
			}
			if m.backoff != test.backoff {
				t.Errorf("unexpected backoff status: %v, want: %v", m.backoff, test.backoff)
			}
			if m.deleteSession != test.deleteSession {
				t.Errorf("unexpected delete session status: %v, want: %v", m.deleteSession, test.deleteSession)
			}
		})
	}
}
