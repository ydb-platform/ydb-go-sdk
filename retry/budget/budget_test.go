package budget

import (
	"context"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestUnlimitedBudget(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx, cancel := xcontext.WithCancel(xtest.Context(t))
		q := Limited(-1)
		require.NoError(t, q.Acquire(ctx))
		cancel()
		require.ErrorIs(t, q.Acquire(ctx), context.Canceled)
	})
}

func TestLimited(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx, cancel := xcontext.WithCancel(xtest.Context(t))
		clock := clockwork.NewFakeClock()
		q := Limited(1, withFixedBudgetClock(clock))
		defer q.Stop()
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

func TestPercent(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		var (
			total   = 1000000
			percent = 0.25
			ctx     = xtest.Context(t)
			b       = Percent(int(percent * 100))
			success int
		)
		for i := 0; i < total; i++ {
			if b.Acquire(ctx) == nil {
				success++
			}
		}
		require.GreaterOrEqual(t, success, int(float64(total)*(percent-0.1*percent)))
		require.LessOrEqual(t, success, int(float64(total)*(percent+0.1*percent)))
	}, xtest.StopAfter(5*time.Second))
}
