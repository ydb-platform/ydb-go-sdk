package topicreaderinternal

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestEmptyCommitResponseDoesNotWaitForCreditObserver(t *testing.T) {
	for _, subscribed := range []bool{false, true} {
		t.Run(map[bool]string{false: "credit-only", true: "credit-and-ack"}[subscribed], func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			started, release := make(chan struct{}), make(chan struct{})
			e.reader.cfg.Trace = &trace.Topic{
				OnReaderCreditBalanceChanged: func(i trace.TopicReaderCreditBalanceChangedInfo) {
					if i.BytesDelta == 1 {
						close(started)
						<-release
					}
				},
			}
			if subscribed {
				e.reader.cfg.Trace.OnReaderCommittedNotify = func(trace.TopicReaderCommittedNotifyInfo) {}
			}
			e.reader.startTrace()
			e.reader.cfg.Trace.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 1})
			defer close(release)
			xtest.WaitChannelClosed(t, started)
			e.Start()
			received := make(chan struct{})
			e.SendFromServerAndSetNextCallback(&rawtopicreader.CommitOffsetResponse{}, func() { close(received) })
			xtest.WaitChannelClosed(t, received)
		})
	}
}
