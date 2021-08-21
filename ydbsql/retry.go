package ydbsql

import ydb "github.com/YandexDatabase/ydb-go-sdk/v2"

type RetryConfig struct {
	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// If backoff is nil, then the DefaultBackoff is used.
	Backoff ydb.Backoff
}

func isBusy(err error) bool {
	return retryChecker.Check(err).MustCheckSession()
}
