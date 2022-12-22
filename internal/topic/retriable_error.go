package topic

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func IsRetryableError(err error) bool {
	mode := retry.Check(err)
	return mode.MustRetry(true)
}

func CheckResetReconnectionCounters(lastTry, now time.Time, connectionTimeout time.Duration) bool {
	const resetAttemptEmpiricalCoefficient = 10
	return now.Sub(lastTry) > connectionTimeout*resetAttemptEmpiricalCoefficient
}

func CheckRetryMode(err error, isPreCheck bool, currentAttempt int, customCheckFunc PublicCheckRetryFunc) (
	_ backoff.Backoff,
	isRetriable bool,
) {
	isRetriable = true

	decision := PublicRetryDecisionDefault
	if customCheckFunc != nil {
		decision = customCheckFunc(NewCheckRetryArgs(isPreCheck, currentAttempt, err))
	}

	switch decision {
	case PublicRetryDecisionDefault:
		isRetriable = IsRetryableError(err)
	case PublicRetryDecisionRetry:
		isRetriable = true
	case PublicRetryDecisionStop:
		isRetriable = false
	default:
		panic(fmt.Errorf("unexpected retry decision: %v", decision))
	}

	if !isRetriable {
		return nil, false
	}

	if retry.Check(err).BackoffType() == backoff.TypeFast {
		return backoff.Fast, true
	}

	return backoff.Slow, true
}
