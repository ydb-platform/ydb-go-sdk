package internal

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3"
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
		backoff ydb.LogBackoff
		exp     []exp
		seeds   int64
	}{
		{
			backoff: ydb.LogBackoff{
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
			backoff: ydb.LogBackoff{
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
			backoff: ydb.LogBackoff{
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
					act := test.backoff.Delay(n)
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
		err           error                // given error
		retryType     RetryType   // type of retry: no retry, retry always idempotent, retry conditionally with user allow retry for unidempotent operations
		backoff       BackoffType // type of Backoff: no Backoff (=== no retry), fast Backoff, slow Backoff
		deleteSession bool                 // close session and delete from pool
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not retry and will close session
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.Canceled, // golang context cancelled
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorUnknownCode,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorCanceled,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorUnknown,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorInvalidArgument,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorDeadlineExceeded,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorNotFound,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorAlreadyExists,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorPermissionDenied,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorResourceExhausted,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorFailedPrecondition,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorAborted,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorOutOfRange,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorUnimplemented,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorInternal,
			},
			retryType:     RetryTypeIdempotent,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorUnavailable,
			},
			retryType:     RetryTypeIdempotent,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorDataLoss,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.TransportError{
				Reason: ydb.TransportErrorUnauthenticated,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusUnknownStatus,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusBadRequest,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusUnauthorized,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusInternalError,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusAborted,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusUnavailable,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusOverloaded,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusSchemeError,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusGenericError,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusTimeout,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusBadSession,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusPreconditionFailed,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusAlreadyExists,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusNotFound,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusSessionExpired,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusCancelled,
			},
			retryType:     RetryTypeIdempotent,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusUndetermined,
			},
			retryType:     RetryTypeIdempotent,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusUnsupported,
			},
			retryType:     RetryTypeNoRetry,
			backoff:       BackoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &ydb.OpError{
				Reason: ydb.StatusSessionBusy,
			},
			retryType:     RetryTypeAny,
			backoff:       BackoffTypeFastBackoff,
			deleteSession: true,
		},
	}
	for _, test := range errs {
		t.Run(test.err.Error(), func(t *testing.T) {
			m := ydb.Check(test.err)
			if m.retry != test.retryType {
				t.Errorf("unexpected retryType status: %v, want: %v", m.retry, test.retryType)
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
