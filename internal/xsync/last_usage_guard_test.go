package xsync

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestLastUsageGuardLock(t *testing.T) {
	t.Run("NowFromLocked", func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := clockwork.NewFakeClockAt(start)
		lu := &lastUsage{
			clock: clock,
		}
		lu.t.Store(&start)
		t1 := lu.Get()
		require.Equal(t, start, t1)
		f := lu.Start()
		clock.Advance(time.Hour)
		t2 := lu.Get()
		require.Equal(t, start.Add(time.Hour), t2)
		clock.Advance(time.Hour)
		f()
		t3 := lu.Get()
		require.Equal(t, start.Add(2*time.Hour), t3)
		clock.Advance(time.Hour)
		t4 := lu.Get()
		require.Equal(t, start.Add(2*time.Hour), t4)
	})
	t.Run("UpdateAfterLastUnlock", func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := clockwork.NewFakeClockAt(start)
		lu := &lastUsage{
			clock: clock,
		}
		lu.t.Store(&start)
		t1 := lu.Get()
		require.Equal(t, start, t1)
		f1 := lu.Start()
		clock.Advance(time.Hour)
		t2 := lu.Get()
		require.Equal(t, start.Add(time.Hour), t2)
		f2 := lu.Start()
		clock.Advance(time.Hour)
		f1()
		f3 := lu.Start()
		clock.Advance(time.Hour)
		t3 := lu.Get()
		require.Equal(t, start.Add(3*time.Hour), t3)
		clock.Advance(time.Hour)
		t4 := lu.Get()
		require.Equal(t, start.Add(4*time.Hour), t4)
		f3()
		t5 := lu.Get()
		require.Equal(t, start.Add(4*time.Hour), t5)
		clock.Advance(time.Hour)
		t6 := lu.Get()
		require.Equal(t, start.Add(5*time.Hour), t6)
		clock.Advance(time.Hour)
		f2()
		t7 := lu.Get()
		require.Equal(t, start.Add(6*time.Hour), t7)
		clock.Advance(time.Hour)
		f2()
		t8 := lu.Get()
		require.Equal(t, start.Add(6*time.Hour), t8)
	})
	t.Run("DeferRelease", func(t *testing.T) {
		start := time.Unix(0, 0)
		clock := clockwork.NewFakeClockAt(start)
		lu := &lastUsage{
			clock: clock,
		}
		lu.t.Store(&start)

		func() {
			t1 := lu.Get()
			require.Equal(t, start, t1)
			clock.Advance(time.Hour)
			t2 := lu.Get()
			require.Equal(t, start, t2)
			clock.Advance(time.Hour)
			defer lu.Start()()
			t3 := lu.Get()
			require.Equal(t, start.Add(2*time.Hour), t3)
			clock.Advance(time.Hour)
			t4 := lu.Get()
			require.Equal(t, start.Add(3*time.Hour), t4)
			clock.Advance(time.Hour)
		}()
		clock.Advance(time.Hour)
		t5 := lu.Get()
		require.Equal(t, start.Add(4*time.Hour), t5)
	})
}
