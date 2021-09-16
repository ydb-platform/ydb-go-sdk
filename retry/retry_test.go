package retry

import (
	"context"
	"fmt"
	errors2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
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
		err           error               // given error
		retryType     errors2.RetryType   // types of retry: no retry, retry always idempotent, retry conditionally with user allow retry for unidempotent operations
		backoff       errors2.BackoffType // types of Backoff: no Backoff (=== no retry), fast Backoff, slow Backoff
		deleteSession bool                // close session and delete from pool
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not retry and will close session
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.Canceled, // golang context cancelled
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorUnknownCode,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorCanceled,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorUnknown,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorInvalidArgument,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorDeadlineExceeded,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorNotFound,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorAlreadyExists,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorPermissionDenied,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorResourceExhausted,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorFailedPrecondition,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorAborted,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorOutOfRange,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorUnimplemented,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorInternal,
			},
			retryType:     errors2.RetryTypeIdempotent,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorUnavailable,
			},
			retryType:     errors2.RetryTypeIdempotent,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorDataLoss,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.TransportError{
				Reason: errors2.TransportErrorUnauthenticated,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusUnknownStatus,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusBadRequest,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusUnauthorized,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusInternalError,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusAborted,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusUnavailable,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusOverloaded,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusSchemeError,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusGenericError,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusTimeout,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusBadSession,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusPreconditionFailed,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusAlreadyExists,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusNotFound,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusSessionExpired,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusCancelled,
			},
			retryType:     errors2.RetryTypeIdempotent,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusUndetermined,
			},
			retryType:     errors2.RetryTypeIdempotent,
			backoff:       errors2.BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusUnsupported,
			},
			retryType:     errors2.RetryTypeNoRetry,
			backoff:       errors2.BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &errors2.OpError{
				Reason: errors2.StatusSessionBusy,
			},
			retryType:     errors2.RetryTypeAny,
			backoff:       errors2.BackoffTypeFastBackoff,
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
