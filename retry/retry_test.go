package retry

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/errors"
	"math/rand"
	"testing"
	"time"
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
	type Case struct {
		err           error              // given error
		retryType     errors.RetryType   // types of retry: no retry, retry always idempotent, retry conditionally with user allow retry for unidempotent operations
		backoff       errors.BackoffType // types of Backoff: no Backoff (=== no retry), fast Backoff, slow Backoff
		deleteSession bool               // close session and delete from pool
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not retry and will close session
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.Canceled, // golang context cancelled
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnknownCode,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorCanceled,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnknown,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorInvalidArgument,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorDeadlineExceeded,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorNotFound,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorAlreadyExists,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorPermissionDenied,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorResourceExhausted,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorFailedPrecondition,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorAborted,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorOutOfRange,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnimplemented,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorInternal,
			},
			retryType:     errors.RetryTypeIdempotent,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnavailable,
			},
			retryType:     errors.RetryTypeIdempotent,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorDataLoss,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.TransportError{
				Reason: errors.TransportErrorUnauthenticated,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusUnknownStatus,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusBadRequest,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusUnauthorized,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusInternalError,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusAborted,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusUnavailable,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusOverloaded,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusSchemeError,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusGenericError,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusTimeout,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusBadSession,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusPreconditionFailed,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusAlreadyExists,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusNotFound,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusSessionExpired,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusCancelled,
			},
			retryType:     errors.RetryTypeIdempotent,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusUndetermined,
			},
			retryType:     errors.RetryTypeIdempotent,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusUnsupported,
			},
			retryType:     errors.RetryTypeNoRetry,
			backoff:       errors.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors.OpError{
				Reason: errors.StatusSessionBusy,
			},
			retryType:     errors.RetryTypeAny,
			backoff:       errors.BackoffTypeFastBackoff,
			deleteSession: true,
		},
	}
	for _, test := range errs {
		t.Run(test.err.Error(), func(t *testing.T) {
			m := Check(test.err)
			if m.retry != test.retryType {
				t.Errorf("unexpected RetryType status: %v, want: %v", m.retry, test.retryType)
			}
			if m.backoff != test.backoff {
				t.Errorf("unexpected Backoff status: %v, want: %v", m.backoff, test.backoff)
			}
			if m.deleteSession != test.deleteSession {
				t.Errorf("unexpected delete session status: %v, want: %v", m.deleteSession, test.deleteSession)
			}
		})
	}
}
