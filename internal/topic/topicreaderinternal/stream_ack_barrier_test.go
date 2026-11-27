package topicreaderinternal

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCommitResponseDoesNotWaitForCreditObserver(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		subscribed, hasOffset bool
	}{
		{name: "credit-only"},
		{name: "credit-and-ack-empty", subscribed: true},
		{name: "credit-and-ack-nonempty", subscribed: true, hasOffset: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.subscribed {
				e.reader.cfg.Trace.OnReaderCommittedNotify = func(trace.TopicReaderCommittedNotifyInfo) {}
			}
			e.reader.startTrace()
			e.reader.cfg.Trace.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: 1})
			defer close(release)
			xtest.WaitChannelClosed(t, started)
			e.Start()
			received := make(chan struct{})
			response := &rawtopicreader.CommitOffsetResponse{}
			if tc.hasOffset {
				response = readerMetricCommitResponse(&e, 21)
			}
			e.SendFromServerAndSetNextCallback(response, func() { close(received) })
			xtest.WaitChannelClosed(t, received)
		})
	}
}
