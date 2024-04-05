package retry

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestUnlimitedLimiter(t *testing.T) {
	ctx, cancel := xcontext.WithCancel(xtest.Context(t))
	q := unlimitedLimiter{}
	require.NoError(t, q.Acquire(ctx))
	cancel()
	require.ErrorIs(t, q.Acquire(ctx), context.Canceled)
}

func TestQuoter(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx, cancel := xcontext.WithCancel(xtest.Context(t))
		clock := clockwork.NewFakeClock()
		q := Quoter(1, func(q *rateLimiter) {
			q.clock = clock
		})
		require.NoError(t, q.Acquire(ctx))
		acquireCh := make(chan struct{})
		go func() {
			err := q.Acquire(ctx)
			acquireCh <- struct{}{}
			require.NoError(t, err)
		}()
		timeCh := make(chan struct{})
		go func() {
			clock.Advance(time.Second - time.Nanosecond)
			timeCh <- struct{}{}
			clock.Advance(time.Nanosecond)
		}()
		select {
		case <-acquireCh:
			require.Fail(t, "")
		case <-timeCh:
		}
		<-acquireCh
		cancel()
		require.ErrorIs(t, q.Acquire(ctx), context.Canceled)
	})
}
