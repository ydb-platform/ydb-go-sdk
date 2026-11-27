package xsync

import (
	"runtime"
	"testing"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestUnboundedChanRangeReleasesDrainedBurst(t *testing.T) {
	const burst = 500_000
	for _, closeDuringBurst := range []bool{true, false} {
		name := "stays_open"
		if closeDuringBurst {
			name = "closes_during_burst"
		}
		t.Run(name, func(t *testing.T) {
			q := NewUnboundedChan[*[128]byte]()
			before := unboundedChanLiveHeap()
			payload := new([128]byte)
			ref := weak.Make(payload)
			for range burst {
				q.Send(payload)
			}
			seen := 0
			if closeDuringBurst {
				q.Range(func(*[128]byte) {
					seen++
					if seen == burst {
						q.Close()
					}
				})
			} else {
				drained, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				defer func() {
					q.Close()
					close(resume)
					xtest.WaitChannelClosed(t, done)
				}()
				go func() {
					defer close(done)
					q.Range(func(msg *[128]byte) {
						if msg == nil {
							// Range has handed back the consumed batch before taking the marker.
							close(drained)
							<-resume

							return
						}
						seen++
						if seen == burst {
							q.Send(nil)
						}
					})
				}()
				xtest.WaitChannelClosed(t, drained)
			}

			retained := unboundedChanLiveHeap() - before
			capacity := cap(q.buffer)
			runtime.KeepAlive(q)
			t.Logf("drained %d items, capacity=%d, retained %d bytes", seen, capacity, retained)
			require.Equal(t, burst, seen)
			require.Nil(t, ref.Value())
			require.Less(t, retained, int64(256<<10))
			require.LessOrEqual(t, capacity, 1024)
		})
	}
}

func TestUnboundedChanRangeDoesNotRecycleAfterClose(t *testing.T) {
	q := NewUnboundedChan[int]()
	q.Send(1)
	var seen []int
	q.Range(func(n int) {
		seen = append(seen, n)
		if n == 1 {
			q.Send(2)
			q.Close()
		}
	})

	require.Equal(t, []int{1, 2}, seen)
	require.Nil(t, q.buffer)
}

func unboundedChanLiveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return int64(stats.HeapAlloc)
}
