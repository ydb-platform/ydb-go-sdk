package topiclistenerinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestListenerEmptyCommitResponseDoesNotWaitForCreditObserver(t *testing.T) {
	for _, subscribed := range []bool{false, true} {
		t.Run(map[bool]string{false: "credit-only", true: "credit-and-ack"}[subscribed], func(t *testing.T) {
			e := fixenv.New(t)
			l := StreamListener(e)
			l.cfg.ReaderInfo.Listener = true
			started, release, next := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx := xtest.Context(t)
			tracer := &trace.Topic{OnListenerCreditBalanceChanged: func(i trace.TopicReaderCreditBalanceChangedInfo) {
				if i.BytesDelta == 1 {
					close(started)
					<-release
				}
			}}
			if subscribed {
				tracer.OnReaderCommittedNotify = func(trace.TopicReaderCommittedNotifyInfo) {}
			}
			l.tracer, l.traceQueue = topicreadercommon.StreamTrace(tracer, l.cfg.ReaderInfo, l.closeOnTracePanic)
			defer func() { require.NoError(t, l.Close(ctx, errors.New("test close"))) }()
			defer close(release)
			l.changeCreditBalance(1)
			xtest.WaitChannelClosed(t, started)
			first := StreamMock(e).EXPECT().Recv().Return(&rawtopicreader.CommitOffsetResponse{}, nil)
			StreamMock(e).EXPECT().Recv().After(first.Call).DoAndReturn(func() (rawtopicreader.ServerMessage, error) {
				close(next)
				<-l.background.Done()

				return nil, context.Canceled
			}).MaxTimes(1)
			l.background.Start("receive", l.receiveMessagesLoop)
			xtest.WaitChannelClosed(t, next)
		})
	}
}
