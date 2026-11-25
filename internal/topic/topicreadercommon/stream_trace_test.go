package topicreadercommon

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamTraceCoalescesStalledACKs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan trace.TopicReaderCommittedNotifyInfo, 2)
	tr, closeQueue := StreamTrace(&trace.Topic{
		OnReaderCommittedNotify: func(i trace.TopicReaderCommittedNotifyInfo) {
			if i.PartitionSessionID == 0 {
				close(started)
				<-release

				return
			}
			select {
			case observed <- i:
			case <-ctx.Done():
			}
		},
	}, ReaderInfo{}, nil)
	defer closeQueue()
	tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("ACK observer did not start")
	}
	const events = 100000
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for n := range int64(events) {
		tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
			PartitionSessionID: 1 + n%2, CommittedOffset: n, MessagesCount: 1,
		})
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	close(release)
	// Two pending sessions need far less than a megabyte, even with runtime noise.
	require.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(1<<20))
	for session := range int64(2) {
		select {
		case got := <-observed:
			require.Equal(t, session+1, got.PartitionSessionID)
			require.Equal(t, events-2+session, got.CommittedOffset)
			require.EqualValues(t, events/2, got.MessagesCount)
		case <-ctx.Done():
			t.Fatal("pending ACK did not drain")
		}
	}
}

func TestStreamTraceDrainsAfterObserverPanic(t *testing.T) {
	retired := make(chan error, 1)
	observations := make(chan int, 3)
	release := make(chan struct{})
	observer := func(i trace.TopicReaderCreditBalanceChangedInfo) {
		observations <- i.BytesDelta
		if i.BytesDelta == 2 {
			<-release
			panic("bad observer")
		}
	}
	tr, closeQueue := StreamTrace(&trace.Topic{OnReaderCreditBalanceChanged: observer}, ReaderInfo{}, func(err error) {
		retired <- err
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// Wait for each update to be in flight so it cannot coalesce with the next.
	for _, n := range []int{1, 2} {
		tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{Context: &ctx, BytesDelta: n})
		select {
		case got := <-observations:
			require.Equal(t, n, got)
		case <-ctx.Done():
			t.Fatal("observer did not start")
		}
	}
	tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{Context: &ctx, BytesDelta: -3})
	closeQueue()
	close(release)
	select {
	case got := <-observations:
		require.Equal(t, -3, got)
	case <-ctx.Done():
		t.Fatal("queue did not drain")
	}
	select {
	case err := <-retired:
		require.ErrorContains(t, err, "bad observer")
	case <-ctx.Done():
		t.Fatal("stream did not retire")
	}
}
