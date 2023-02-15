package xsync

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
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
		events := int64(0)

		backgroundCounter := int64(0)
		firstWaiterStarted := xatomic.Bool{}

		stopSubscribe := xatomic.Bool{}

		subscribeStopped := make(empty.Chan)
		broadcastStopped := make(empty.Chan)

		// Add subscribers
		go func() {
			defer close(subscribeStopped)
			for {
				atomic.AddInt64(&backgroundCounter, 1)
				waiter := b.Waiter()
				firstWaiterStarted.Store(true)
				go func() {
					<-waiter.Done()
					atomic.AddInt64(&backgroundCounter, -1)
				}()
				if stopSubscribe.Load() {
					return
				}
			}
		}()

		stopBroadcast := xatomic.Bool{}
		go func() {
			defer close(broadcastStopped)

			// Fire events
			for {
				atomic.AddInt64(&events, 1)
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
			oldCounter := atomic.LoadInt64(&backgroundCounter)
			if oldCounter == 0 {
				break
			}

			t.Log("background counter", oldCounter)
			xtest.SpinWaitCondition(t, nil, func() bool {
				return atomic.LoadInt64(&backgroundCounter) < oldCounter
			})
		}
		stopBroadcast.Store(true)
		<-broadcastStopped

		require.True(t, atomic.LoadInt64(&events) > 0)
	})
}
