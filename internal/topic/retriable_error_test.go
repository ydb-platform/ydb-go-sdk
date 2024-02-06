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

func TestCheckRetryMode_OK(t *testing.T) {
	err := error(nil)
	settings := RetrySettings{}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(err, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_RetryRetriableErrorFast(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	settings := RetrySettings{}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Fast, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_RetryRetriableErrorFastWithTimeout(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	settings := RetrySettings{
		StartTimeout: time.Second,
	}
	duration := time.Second * 2

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_RetryRetriableErrorSlow(t *testing.T) {
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	settings := RetrySettings{}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(slowError, settings, duration)
	require.Equal(t, backoff.Slow, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_RetryRetriableErrorSlowWithTimeout(t *testing.T) {
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	settings := RetrySettings{
		StartTimeout: time.Second,
	}
	duration := time.Second * 2

	resBackoff, retriable := CheckRetryMode(slowError, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UnretriableError(t *testing.T) {
	unretriable := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	settings := RetrySettings{}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(unretriable, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UserOverrideFastErrorDefault(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionDefault
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Fast, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_UserOverrideFastErrorRetry(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionRetry
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Fast, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_UserOverrideFastErrorStop(t *testing.T) {
	fastError := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, ""))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionStop
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UserOverrideSlowErrorDefault(t *testing.T) {
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionDefault
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(slowError, settings, duration)
	require.Equal(t, backoff.Slow, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_UserOverrideSlowErrorRetry(t *testing.T) {
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionRetry
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(slowError, settings, duration)
	require.Equal(t, backoff.Slow, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_UserOverrideSlowErrorStop(t *testing.T) {
	slowError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionStop
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(slowError, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UserOverrideUnretriableErrorDefault(t *testing.T) {
	unretriable := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionDefault
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(unretriable, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UserOverrideUnretriableErrorRetry(t *testing.T) {
	unretriable := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionRetry
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(unretriable, settings, duration)
	require.Equal(t, backoff.Slow, resBackoff)
	require.Equal(t, true, retriable)
}

func TestCheckRetryMode_UserOverrideUnretriableErrorStop(t *testing.T) {
	unretriable := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	settings := RetrySettings{
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionStop
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(unretriable, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_UserOverrideFastErrorRetryWithTimeout(t *testing.T) {
	fastError := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	settings := RetrySettings{
		StartTimeout: time.Second,
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			return PublicRetryDecisionRetry
		},
	}
	duration := time.Second * 2

	resBackoff, retriable := CheckRetryMode(fastError, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_NotCallForNil(t *testing.T) {
	settings := RetrySettings{
		StartTimeout: time.Second,
		CheckError: func(errInfo PublicCheckErrorRetryArgs) PublicCheckRetryResult {
			panic("must not call for nil err")
		},
	}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(nil, settings, duration)
	require.Equal(t, backoff.Backoff(nil), resBackoff)
	require.Equal(t, false, retriable)
}

func TestCheckRetryMode_EOF(t *testing.T) {
	eofError := fmt.Errorf("test wrap: %w", io.EOF)
	settings := RetrySettings{}
	duration := time.Duration(0)

	resBackoff, retriable := CheckRetryMode(eofError, settings, duration)
	require.Equal(t, backoff.Slow, resBackoff)
	require.Equal(t, true, retriable)
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
