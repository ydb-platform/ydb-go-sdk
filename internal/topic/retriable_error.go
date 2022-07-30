package topic

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func IsRetryableError(err error) bool {
	mode := retry.Check(err)
	return mode.MustRetry(true)
}
