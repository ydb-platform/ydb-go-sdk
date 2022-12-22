package topic

import (
	"time"

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
