package ydb

import (
	"context"
	"fmt"
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
		err           error       // given error
		retryType     retryType   // type of retry: no retry, retry always idempotent, retry conditionally with user allow retry for unidempotent operations
		backoff       backoffType // type of Backoff: no Backoff (=== no retry), fast Backoff, slow Backoff
		deleteSession bool        // close session and delete from pool
	}
	errs := []Case{
		{
			err:           fmt.Errorf("unknown error"), // retryer given unknown error - we will not retry and will close session
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.DeadlineExceeded, // golang context deadline exceeded
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err:           context.Canceled, // golang context cancelled
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnknownCode,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorCanceled,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnknown,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorInvalidArgument,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorDeadlineExceeded,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorNotFound,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorAlreadyExists,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorPermissionDenied,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorResourceExhausted,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &TransportError{
				Reason: TransportErrorFailedPrecondition,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorAborted,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorOutOfRange,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnimplemented,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorInternal,
			},
			retryType:     retryTypeIdempotent,
			backoff:       backoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnavailable,
			},
			retryType:     retryTypeIdempotent,
			backoff:       backoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorDataLoss,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &TransportError{
				Reason: TransportErrorUnauthenticated,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &OpError{
				Reason: StatusUnknownStatus,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusBadRequest,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusUnauthorized,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusInternalError,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusAborted,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusUnavailable,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusOverloaded,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeSlowBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusSchemeError,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusGenericError,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusTimeout,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusBadSession,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeFastBackoff,
			deleteSession: true,
		},
		{
			err: &OpError{
				Reason: StatusPreconditionFailed,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusAlreadyExists,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusNotFound,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusSessionExpired,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: true,
		},
		{
			err: &OpError{
				Reason: StatusCancelled,
			},
			retryType:     retryTypeIdempotent,
			backoff:       backoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusUndetermined,
			},
			retryType:     retryTypeIdempotent,
			backoff:       backoffTypeFastBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusUnsupported,
			},
			retryType:     retryTypeNoRetry,
			backoff:       backoffTypeNoBackoff,
			deleteSession: false,
		},
		{
			err: &OpError{
				Reason: StatusSessionBusy,
			},
			retryType:     retryTypeAny,
			backoff:       backoffTypeFastBackoff,
			deleteSession: true,
		},
	}
	for _, test := range errs {
		t.Run(test.err.Error(), func(t *testing.T) {
			m := Check(test.err)
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
