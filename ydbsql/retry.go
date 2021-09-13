package ydbsql

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type RetryConfig struct {
	// MaxRetries is a number of maximum attempts to retry a failed operation.
	// If MaxRetries is zero then no attempts will be made.
	MaxRetries int

	// RetryChecker contains options of mapping errors to retry mode.
	RetryChecker ydb.RetryChecker

	// Backoff is a selected backoff policy.
	// Deprecated: use pair FastBackoff / SlowBackoff instead
	Backoff ydb.Backoff

	// FastBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultFastBackoff is used.
	FastBackoff ydb.Backoff

	// SlowBackoff is a selected backoff policy.
	// If backoff is nil, then the ydb.DefaultSlowBackoff is used.
	SlowBackoff ydb.Backoff

	// FastSlot is an init slot for fast retry
	// If FastSlot is zero then the ydb.DefaultFastSlot is used.
	FastSlot time.Duration

	// SlowSlot is as zero then the ydb.DefaultSlowSlot is used.
	SlowSlot time.Duration
}

func backoff(ctx context.Context, m ydb.RetryMode, rc *RetryConfig, i int) error {
	var b ydb.Backoff
	switch m.BackoffType() {
	case ydb.BackoffTypeNoBackoff:
		return nil
	case ydb.BackoffTypeFastBackoff:
		if rc.FastBackoff != nil {
			b = rc.FastBackoff
		} else {
			b = rc.Backoff
		}
	case ydb.BackoffTypeSlowBackoff:
		if rc.SlowBackoff != nil {
			b = rc.SlowBackoff
		} else {
			b = rc.Backoff
		}
	}
	return ydb.WaitBackoff(ctx, b, i)
}
