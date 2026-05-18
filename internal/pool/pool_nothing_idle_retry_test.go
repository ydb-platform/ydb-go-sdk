package pool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

// errNothingIdleItems from getItem (idle pop failed, createItem returns it) is retriable in try
// and must be retried by pool.With when the context is still active.
func TestPoolWith_retriesOnNothingIdleItems(t *testing.T) {
	t.Parallel()

	var (
		createCalls  atomic.Int32
		callbackRuns atomic.Int32
		attempts     atomic.Int32
	)

	trace := *defaultTrace
	trace.OnWith = func(_ *context.Context, _ stack.Caller) func(int, error) {
		return func(a int, err error) {
			attempts.Store(int32(a))
		}
	}

	p := mustNewPool(t,
		WithLimit[*testItem, testItem](1),
		WithTrace[*testItem, testItem](&trace),
		WithCreateItemFunc(func(context.Context) (*testItem, error) {
			if createCalls.Add(1) == 1 {
				return nil, errNothingIdleItems
			}

			return &testItem{}, nil
		}),
	)
	defer mustClose(t, p)

	err := p.With(t.Context(), func(context.Context, *testItem) error {
		callbackRuns.Add(1)

		return nil
	}, retry.WithFastBackoff(testutil.BackoffFunc(func(int) <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch
	})))
	require.NoError(t, err)
	require.GreaterOrEqual(t, attempts.Load(), int32(2))
	require.Equal(t, int32(2), createCalls.Load())
	require.Equal(t, int32(1), callbackRuns.Load())
	requirePoolStats(t, p, poolStats(1, func(s *Stats) { s.Size = 1; s.Idle = 1 }))
}

func TestPoolWith_doesNotRetryOnNothingIdleItemsWhenContextCanceled(t *testing.T) {
	t.Parallel()

	backoffCh := make(chan chan time.Time)
	ctx, cancel := xcontext.WithCancel(t.Context())

	var createCalls atomic.Int32
	p, err := New(ctx,
		WithLimit[*testItem, testItem](1),
		WithCreateItemFunc(func(context.Context) (*testItem, error) {
			createCalls.Add(1)

			return nil, errNothingIdleItems
		}),
	)
	require.NoError(t, err)
	requirePoolStats(t, p, poolStats(1, nil))

	results := make(chan error, 1)
	go func() {
		results <- p.With(ctx, func(context.Context, *testItem) error {
			return nil
		}, retry.WithFastBackoff(testutil.BackoffFunc(func(int) <-chan time.Time {
			ch := make(chan time.Time)
			backoffCh <- ch

			return ch
		})))
	}()

	var backoffTimer chan time.Time
	select {
	case backoffTimer = <-backoffCh:
	case res := <-results:
		t.Fatalf("unexpected early result: %v", res)
	}

	cancel()
	backoffTimer <- time.Now()

	select {
	case err := <-results:
		require.ErrorIs(t, err, context.Canceled)
		require.GreaterOrEqual(t, createCalls.Load(), int32(1))
		require.ErrorIs(t, err, errNothingIdleItems)
	case <-time.After(time.Second):
		t.Fatal("pool.With did not finish after context cancel")
	}
}

func TestNothingIdleItems_isFastRetryable(t *testing.T) {
	t.Parallel()

	re := xerrors.RetryableError(errNothingIdleItems)
	require.NotNil(t, re)
	require.Equal(t, backoff.TypeFast, re.BackoffType())
}
