package ydbsql

import (
	"context"

	"github.com/yandex-cloud/ydb-go-sdk"
)

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

// retry calls f until it return nil or not retriable error.
func retry(ctx context.Context, r *RetryConfig, f func(context.Context) error) (err error) {
	for i := 0; i <= r.MaxRetries; i++ {
		if err = f(ctx); err == nil {
			return nil
		}
		m := r.RetryChecker.Check(err)
		if !m.Retriable() {
			return err
		}
		if m.MustDeleteSession() {
			return err
		}
		if m.MustBackoff() {
			if e := ydb.WaitBackoff(ctx, r.Backoff, i); e != nil {
				// Return original error to make it possible to lay on for the
				// client.
				return err
			}
		}
	}
	return err
}
