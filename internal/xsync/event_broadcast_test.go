package xsync

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestEventBroadcast(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		b := &EventBroadcast{}
		waiter := b.Waiter()
		b.Broadcast()
		xtest.WaitChannelClosed(t, waiter.Done())
	})

	xtest.TestManyTimesWithName(t, "SubscribeAndEventsInRace", func(t testing.TB) {
		testDuration := time.Second / 100

		b := &EventBroadcast{}
		var events atomic.Int64

		var backgroundCounter atomic.Int64
		firstWaiterStarted := atomic.Bool{}

		stopSubscribe := atomic.Bool{}

		subscribeStopped := make(empty.Chan)
		broadcastStopped := make(empty.Chan)

		// Add subscribers
		go func() {
			defer close(subscribeStopped)
			for {
				backgroundCounter.Add(1)
				waiter := b.Waiter()
				firstWaiterStarted.Store(true)
				go func() {
					<-waiter.Done()
					backgroundCounter.Add(-1)
				}()
				if stopSubscribe.Load() {
					return
				}
			}
		}()

		stopBroadcast := atomic.Bool{}
		go func() {
			defer close(broadcastStopped)

			// Fire events
			for {
				events.Add(1)
				b.Broadcast()
				runtime.Gosched()
				if stopBroadcast.Load() {
					return
				}
			}
		}()

		xtest.SpinWaitCondition(t, nil, firstWaiterStarted.Load)

		<-time.After(testDuration)

		stopSubscribe.Store(true)
		<-subscribeStopped

		for {
			oldCounter := backgroundCounter.Load()
			if oldCounter == 0 {
				break
			}

			t.Log("background counter", oldCounter)
			xtest.SpinWaitCondition(t, nil, func() bool {
				return backgroundCounter.Load() < oldCounter
			})
		}
		stopBroadcast.Store(true)
		<-broadcastStopped

		require.Greater(t, events.Load(), int64(0))
	})
}
