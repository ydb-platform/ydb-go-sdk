package topicreadercommon

import (
	"runtime"
	"strconv"
	"testing"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

func TestStreamTraceReleasesPendingHeapAfterDrain(t *testing.T) {
	const burst = 500_000
	type event struct {
		count   int
		payload *[128]byte
	}
	for _, tc := range []struct {
		name     string
		distinct bool
	}{
		{name: "one_topic"},
		{name: "distinct_topics", distinct: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &StreamTraceQueue{work: xsync.NewUnboundedChan[func()]()}
			delivered := 0
			send := coalesceTrace[string](q, func(v event) { delivered += v.count }, func(a, b event) event {
				a.count += b.count

				return a
			})
			before := commitTrackerLiveHeap()
			payload := new([128]byte)
			ref := weak.Make(payload)
			for i := range burst {
				key := "same-topic"
				if tc.distinct {
					key = strconv.Itoa(i)
				}
				send(key, event{count: 1, payload: payload})
			}
			payload = nil

			q.Close()
			q.work.Range(func(f func()) { f() })

			retained := commitTrackerLiveHeap() - before
			runtime.KeepAlive(send)
			t.Logf("drained %d updates, retained %d bytes", delivered, retained)
			require.Equal(t, burst, delivered)
			require.Nil(t, ref.Value())
			require.Less(t, retained, int64(256<<10))
		})
	}
}

func TestStreamTraceCoalescesAgainAfterDrainingManyTopics(t *testing.T) {
	const topics = 128
	q := &StreamTraceQueue{work: xsync.NewUnboundedChan[func()]()}
	var delivered []int
	send := coalesceTrace[int](q, func(n int) { delivered = append(delivered, n) }, func(a, b int) int { return a + b })
	for key := range topics {
		send(key, 1)
	}
	for _, callback := range q.work.DrainBuffered() {
		callback()
	}
	delivered = nil

	send(0, 2)
	send(0, 3)
	q.Close()
	q.work.Range(func(f func()) { f() })

	require.Equal(t, []int{5}, delivered)
}
