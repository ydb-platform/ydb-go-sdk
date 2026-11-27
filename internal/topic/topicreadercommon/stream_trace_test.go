package topicreadercommon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamTraceSkipsQueueWithDisabledTopicLogging(t *testing.T) {
	logTrace := log.Topic(log.Default(io.Discard), trace.Details(0))
	cfg := topic.Config{}
	composed := cfg.ComposeTrace(&trace.Topic{}, &logTrace)

	for _, tc := range []struct {
		name     string
		listener bool
	}{
		{name: "pull reader"},
		{name: "listener", listener: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, queue := StreamTrace(composed, ReaderInfo{Listener: tc.listener}, nil)
			if queue != nil {
				t.Cleanup(queue.Close)
			}
			require.Nil(t, queue)
		})
	}
}

func TestStreamTraceKeepsEachACKOffset(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	observed := make(chan int64, 2)
	tr, queue := StreamTrace(&trace.Topic{
		OnReaderCreditBalanceChanged: func(trace.TopicReaderCreditBalanceChangedInfo) {
			close(started)
			<-release
		},
		OnReaderCommittedNotify: func(i trace.TopicReaderCommittedNotifyInfo) {
			observed <- i.CommittedOffset
		},
	}, ReaderInfo{}, nil)
	defer queue.Close()
	tr.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{})
	<-started
	for _, offset := range []int64{10, 20} {
		tr.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{PartitionSessionID: 1, CommittedOffset: offset})
	}
	close(release)
	for _, want := range []int64{10, 20} {
		require.Equal(t, want, <-observed)
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
	tr, queue := StreamTrace(&trace.Topic{OnReaderCreditBalanceChanged: observer}, ReaderInfo{}, func(err error) {
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
	queue.Close()
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
