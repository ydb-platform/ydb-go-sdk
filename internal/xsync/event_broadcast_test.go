package xsync

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestEventBroadcast(t *testing.T) {
	t.Run("Simple", simpleTest)

	xtest.TestManyTimesWithName(t, "SubscribeAndEventsInRace", func(t testing.TB) {
		testDuration := time.Second / 100
		b := &EventBroadcast{}
		var events xatomic.Int64

		var backgroundCounter xatomic.Int64
		firstWaiterStarted := xatomic.Bool{}
		stopSubscribe := xatomic.Bool{}
		subscribeStopped := make(empty.Chan)

		stopBroadcast := xatomic.Bool{}
		broadcastStopped := make(empty.Chan)

		go subscribeHelper(b, &stopSubscribe, &firstWaiterStarted, &backgroundCounter, subscribeStopped)
		go broadcastHelper(b, &events, &stopBroadcast, broadcastStopped)

		xtest.SpinWaitCondition(t, nil, firstWaiterStarted.Load)
		<-time.After(testDuration)

		stopSubscribe.Store(true)
		<-subscribeStopped

		waitForBackgroundCounterToZero(t, &backgroundCounter)

		stopBroadcast.Store(true)
		<-broadcastStopped

		require.Greater(t, events.Load(), int64(0))
	})
}

func simpleTest(t *testing.T) {
	b := &EventBroadcast{}
	waiter := b.Waiter()
	b.Broadcast()
	xtest.WaitChannelClosed(t, waiter.Done())
}

func subscribeHelper(b *EventBroadcast, stopSubscribe *xatomic.Bool, firstWaiterStarted *xatomic.Bool, backgroundCounter *xatomic.Int64, subscribeStopped empty.Chan) {
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
}

func broadcastHelper(b *EventBroadcast, events *xatomic.Int64, stopBroadcast *xatomic.Bool, broadcastStopped empty.Chan) {
	defer close(broadcastStopped)
	for {
		events.Add(1)
		b.Broadcast()
		runtime.Gosched()
		if stopBroadcast.Load() {
			return
		}
	}
}

func waitForBackgroundCounterToZero(t testing.TB, backgroundCounter *xatomic.Int64) {
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
}
