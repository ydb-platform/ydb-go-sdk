package topic

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func IsRetryableError(err error) bool {
	if xerrors.RetryableError(err) != nil {
		return true
	}
	mode := retry.Check(err)
	return mode.MustRetry(true)
}
