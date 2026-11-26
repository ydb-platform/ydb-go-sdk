package topicreadercommon

import (
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
