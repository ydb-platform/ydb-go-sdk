package spans

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// fastBackoffOption is a no-op fast backoff for tests.
type zeroBackoff struct{}

func (zeroBackoff) Delay(_ int) time.Duration { return 0 }

func TestRetrySpansEmitRunWithRetryAndTry(t *testing.T) {
	adapter := &recordingAdapter{}
	tr := Retry(adapter)

	ctx := context.Background()
	want := errors.New("permanent")
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return want
	},
		retry.WithTrace(&tr),
		retry.WithFastBackoff(zeroBackoff{}),
		retry.WithSlowBackoff(zeroBackoff{}),
	)
	require.Error(t, err)

	loops := adapter.byName(SpanNameRunWithRetry)
	require.Len(t, loops, 1, "expected single ydb.RunWithRetry span")
	require.True(t, loops[0].ended)
	require.NotNil(t, loops[0].err, "ydb.RunWithRetry must record error on non-retryable failure")
	// Non-retryable error (plain error not idempotent) -> single attempt.
	tries := adapter.byName(SpanNameTry)
	require.Len(t, tries, 1)
	require.NotNil(t, tries[0].err)
	require.Nil(t, tries[0].attr(AttrYDBRetryBackoffMs),
		"first ydb.Try must NOT carry ydb.retry.backoff_ms (no preceding sleep)")
}

func TestRetrySpansAttemptHasBackoffMs(t *testing.T) {
	adapter := &recordingAdapter{}
	tr := Retry(adapter)

	ctx := context.Background()
	var calls int
	err := retry.Retry(ctx, func(ctx context.Context) error {
		calls++
		if calls < 3 {
			return retry.RetryableError(
				fmt.Errorf("retry %d", calls),
				retry.WithBackoff(retry.TypeFastBackoff),
			)
		}

		return nil
	},
		retry.WithIdempotent(true),
		retry.WithTrace(&tr),
		retry.WithFastBackoff(backoff.New(
			backoff.WithSlotDuration(time.Millisecond),
			backoff.WithCeiling(2),
			backoff.WithJitterLimit(1),
		)),
		retry.WithSlowBackoff(zeroBackoff{}),
	)
	require.NoError(t, err)
	require.Equal(t, 3, calls)

	tries := adapter.byName(SpanNameTry)
	require.Len(t, tries, 3)
	// 1st attempt: no preceding sleep => backoff attribute must be absent.
	require.Nil(t, tries[0].attr(AttrYDBRetryBackoffMs),
		"first ydb.Try must NOT carry ydb.retry.backoff_ms")
	// 2nd and 3rd attempts: backoff_ms must be set and > 0.
	for i := 1; i < len(tries); i++ {
		got, ok := tries[i].attr(AttrYDBRetryBackoffMs).(int64)
		require.True(t, ok, "attempt %d: ydb.retry.backoff_ms must be int64", i+1)
		require.Greater(t, got, int64(0), "attempt %d backoff must be > 0", i+1)
	}

	// The retry loop overall should not be flagged with an error.
	loops := adapter.byName(SpanNameRunWithRetry)
	require.Len(t, loops, 1)
	require.Nil(t, loops[0].err)
}

func TestRetrySpansContextCancelMarksError(t *testing.T) {
	adapter := &recordingAdapter{}
	tr := Retry(adapter)

	ctx, cancel := context.WithCancel(context.Background())

	// Always-retryable error so the loop hits the cancellation path.
	err := retry.Retry(ctx, func(ctx context.Context) error {
		// Cancel right after we've been called once, while in the
		// retry sleep / next-attempt selection.
		cancel()

		return retry.RetryableError(
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED)),
			retry.WithBackoff(retry.TypeFastBackoff),
		)
	},
		retry.WithIdempotent(true),
		retry.WithTrace(&tr),
		retry.WithFastBackoff(zeroBackoff{}),
		retry.WithSlowBackoff(zeroBackoff{}),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)

	loops := adapter.byName(SpanNameRunWithRetry)
	require.Len(t, loops, 1)
	require.NotNil(t, loops[0].err, "ydb.RunWithRetry must record error on context cancel")

	tries := adapter.byName(SpanNameTry)
	require.NotEmpty(t, tries)
	last := tries[len(tries)-1]
	require.NotNil(t, last.err, "last ydb.Try must record the failing error")
}
