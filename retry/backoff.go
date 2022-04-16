package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"time"
)

var (
	// FastBackoff is a default fast backoff object
	//
	// Deprecated: don't use explicit it, will be removed at next major release.
	// Use retry.Backoff constructor instead
	FastBackoff = backoff.Fast

	// SlowBackoff is a default fast backoff object
	//
	// Deprecated: don't use explicit it, will be removed at next major release.
	// Use retry.Backoff constructor instead
	SlowBackoff = backoff.Slow
)

// Backoff makes backoff object with custom params
func Backoff(slotDuration time.Duration, ceiling uint, jitterLimit float64) backoff.Backoff {
	return backoff.New(
		backoff.WithSlotDuration(slotDuration),
		backoff.WithCeiling(ceiling),
		backoff.WithJitterLimit(jitterLimit),
	)
}
