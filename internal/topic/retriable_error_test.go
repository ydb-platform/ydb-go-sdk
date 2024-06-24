package topic

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestCheckRetryMode(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	unretriable := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))

	table := []struct {
		name         string
		err          error
		settings     RetrySettings
		duration     time.Duration
		resBackoff   backoff.Backoff
		resRetriable bool
	}{
		{
			name:         "OK",
			err:          nil,
			settings:     RetrySettings{},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name:         "RetryRetriableErrorFast",
			err:          fastError,
			settings:     RetrySettings{},
			duration:     0,
			resBackoff:   backoff.Fast,
			resRetriable: true,
		},
		{
			name: "RetryRetriableErrorFastWithTimeout",
			err:  fastError,
			settings: RetrySettings{
				StartTimeout: time.Second,
			},
			duration:     time.Second * 2,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name:         "RetryRetriableErrorSlow",
			err:          slowError,
			settings:     RetrySettings{},
			duration:     0,
			resBackoff:   backoff.Slow,
			resRetriable: true,
		},
		{
			name: "RetryRetriableErrorSlowWithTimeout",
			err:  slowError,
			settings: RetrySettings{
				StartTimeout: time.Second,
			},
			duration:     time.Second * 2,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name:         "UnretriableError",
			err:          unretriable,
			settings:     RetrySettings{},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "UserOverrideFastErrorDefault",
			err:  fastError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionDefault
				},
			},
			duration:     0,
			resBackoff:   backoff.Fast,
			resRetriable: true,
		},
		{
			name: "UserOverrideFastErrorRetry",
			err:  fastError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionRetry
				},
			},
			duration:     0,
			resBackoff:   backoff.Fast,
			resRetriable: true,
		},
		{
			name: "UserOverrideFastErrorStop",
			err:  fastError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionStop
				},
			},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "UserOverrideSlowErrorDefault",
			err:  slowError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionDefault
				},
			},
			duration:     0,
			resBackoff:   backoff.Slow,
			resRetriable: true,
		},
		{
			name: "UserOverrideSlowErrorRetry",
			err:  slowError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionRetry
				},
			},
			duration:     0,
			resBackoff:   backoff.Slow,
			resRetriable: true,
		},
		{
			name: "UserOverrideSlowErrorStop",
			err:  slowError,
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionStop
				},
			},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "UserOverrideUnretriableErrorDefault",
			err:  xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionDefault
				},
			},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "UserOverrideUnretriableErrorRetry",
			err:  xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionRetry
				},
			},
			duration:     0,
			resBackoff:   backoff.Slow,
			resRetriable: true,
		},
		{
			name: "UserOverrideUnretriableErrorStop",
			err:  xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
			settings: RetrySettings{
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionStop
				},
			},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "UserOverrideFastErrorRetryWithTimeout",
			err:  xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED)),
			settings: RetrySettings{
				StartTimeout: time.Second,
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					return PublicRetryDecisionRetry
				},
			},
			duration:     time.Second * 2,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name: "NotCallForNil",
			err:  nil,
			settings: RetrySettings{
				StartTimeout: time.Second,
				CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
					panic("must not call for nil err")
				},
			},
			duration:     0,
			resBackoff:   nil,
			resRetriable: false,
		},
		{
			name:         "EOF", // Issue https://github.com/ydb-platform/ydb-go-sdk/issues/754
			err:          fmt.Errorf("test wrap: %w", io.EOF),
			settings:     RetrySettings{},
			duration:     0,
			resBackoff:   backoff.Slow,
			resRetriable: true,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			resBackoff, stopReason := RetryDecision(test.err, test.settings, test.duration)
			require.Equal(t, test.resBackoff, resBackoff)
			require.Equal(t, test.resRetriable, stopReason == nil)
		})
	}
}

func TestCheckResetReconnectionCounters(t *testing.T) {
	now := time.Now()
	table := []struct {
		name              string
		lastTry           time.Time
		connectionTimeout time.Duration
		shouldReset       bool
	}{
		{
			name:              "RecentLastTryWithInfiniteConnectionTimeout",
			lastTry:           now.Add(-30 * time.Second),
			connectionTimeout: value.InfiniteDuration,
			shouldReset:       false,
		},
		{
			name:              "OldLastTryWithInfiniteConnectionTimeout",
			lastTry:           now.Add(-30 * time.Minute),
			connectionTimeout: value.InfiniteDuration,
			shouldReset:       true,
		},
		{
			name:              "LastTryLessThanConnectionTimeout",
			lastTry:           now.Add(-30 * time.Second),
			connectionTimeout: time.Minute,
			shouldReset:       false,
		},
		{
			name:              "LastTryGreaterThanConnectionTimeout",
			lastTry:           now.Add(-time.Hour),
			connectionTimeout: time.Minute,
			shouldReset:       true,
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			shouldReset := CheckResetReconnectionCounters(test.lastTry, now, test.connectionTimeout)
			require.Equal(t, test.shouldReset, shouldReset)
		})
	}
}
