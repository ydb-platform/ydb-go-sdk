package topicreadercommon

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamTraceACKBarrierDoesNotAllocate(t *testing.T) {
	for _, listener := range []bool{false, true} {
		_, q := StreamTrace(&trace.Topic{
			OnReaderCommittedNotify: func(trace.TopicReaderCommittedNotifyInfo) {},
		}, ReaderInfo{Listener: listener}, nil)
		stop := make(chan struct{})
		ok := true
		allocs := testing.AllocsPerRun(1000, func() {
			q.ackQueued.Store(true)
			ok = q.WaitForACKs(stop) && ok
		})
		q.Close()
		require.True(t, ok)
		require.Zero(t, allocs, "listener=%t", listener)
	}
}

func TestStreamTraceACKBarrierIgnoresStalledWorkerObservers(t *testing.T) {
	for _, listener := range []bool{false, true} {
		started, release := make(chan struct{}), make(chan struct{})
		credit := func(i trace.TopicReaderCreditBalanceChangedInfo) {
			if i.BytesDelta == 1 {
				close(started)
				<-release
			}
		}
		acked := make(chan int64, 1)
		tr, q := StreamTrace(&trace.Topic{
			OnReaderCreditBalanceChanged:   credit,
			OnListenerCreditBalanceChanged: credit,
			OnReaderCommittedNotify: func(i trace.TopicReaderCommittedNotifyInfo) {
				acked <- i.CommittedOffset
			},
		}, ReaderInfo{Listener: listener}, nil)
		if listener {
			tr.OnListenerCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 1})
		} else {
			tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 1})
		}
		xtest.WaitChannelClosed(t, started)

		tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{CommittedOffset: 7})
		waited := make(chan struct{})
		go func() {
			defer close(waited)
			require.True(t, q.WaitForACKs(make(chan struct{})))
		}()
		xtest.WaitChannelClosed(t, waited)
		require.EqualValues(t, 7, xtest.Receive(t, acked, "ACK observer"))

		close(release)
		q.Close()
	}
}

func TestStreamTraceReportsOnePanicAcrossLanes(t *testing.T) {
	var reports atomic.Int32
	workTaken, workDone, ackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tr, q := StreamTrace(&trace.Topic{
		OnReaderCreditBalanceChanged: func(i trace.TopicReaderCreditBalanceChangedInfo) {
			if i.BytesDelta == 1 {
				close(workTaken)
				panic("work lane")
			}
			close(workDone)
		},
		OnReaderCommittedNotify: func(i trace.TopicReaderCommittedNotifyInfo) {
			if i.CommittedOffset == 1 {
				panic("ack lane")
			}
			close(ackDone)
		},
	}, ReaderInfo{}, func(error) { reports.Add(1) })
	defer q.Close()

	tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 1})
	tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{CommittedOffset: 1})
	// Each lane is a FIFO: once its follow-up event runs, its panic has been reported.
	// The first credit event must be taken first, or coalescing would merge the two.
	xtest.WaitChannelClosed(t, workTaken)
	tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 2})
	tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{CommittedOffset: 2})
	xtest.WaitChannelClosed(t, workDone)
	xtest.WaitChannelClosed(t, ackDone)
	require.EqualValues(t, 1, reports.Load())
}
