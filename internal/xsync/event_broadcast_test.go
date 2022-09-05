package xsync

import (
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
		ch := b.Subscribe()
		b.Broadcast()
		xtest.WaitChannelClosed(t, ch)
	})

	xtest.TestManyTimesWithName(t, "SubscribeAndEventsInRace", func(t testing.TB) {
		testDuration := time.Second / 100

		b := &EventBroadcast{}
		events := int64(0)

		backgroundCounter := int64(0)

		stopRace := int64(0)

		subscribeStopped := make(empty.Chan)
		fireStopped := make(empty.Chan)

		// Add subscribers
		go func() {
			defer close(subscribeStopped)
			for {
				atomic.AddInt64(&backgroundCounter, 1)
				subscriber := b.Subscribe()
				go func() {
					<-subscriber
					atomic.AddInt64(&backgroundCounter, -1)
				}()
				if atomic.LoadInt64(&stopRace) != 0 {
					return
				}
			}
		}()

		go func() {
			defer close(fireStopped)

			// Fire events
			for {
				atomic.AddInt64(&events, 1)
				b.Broadcast()
				if atomic.LoadInt64(&stopRace) != 0 {
					return
				}
			}
		}()

		<-time.After(testDuration)

		require.True(t, atomic.LoadInt64(&backgroundCounter) > 0)

		atomic.AddInt64(&stopRace, 1)
		<-subscribeStopped
		<-fireStopped
		b.Broadcast()
		xtest.SpinWaitCondition(t, nil, func() bool {
			return atomic.LoadInt64(&backgroundCounter) == 0
		})

		require.True(t, atomic.LoadInt64(&events) > 0)
	})
}
