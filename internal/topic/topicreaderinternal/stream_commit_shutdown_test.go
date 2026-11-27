package topicreaderinternal

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicReaderCompletesACKTraceBeforeReceivingNextResponse(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	var offset atomic.Int64
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCommittedNotify: func(info trace.TopicReaderCommittedNotifyInfo) {
			offset.Store(info.CommittedOffset)
		},
	}
	e.reader.startTrace()
	e.Start()
	observed := make(chan int64, 1)
	for want := int64(21); want < 121; want++ {
		e.SendFromServerAndSetNextCallback(readerMetricCommitResponse(&e, want), func() { observed <- offset.Load() })
		require.Equal(t, want, xtest.Receive(t, observed, "ACK offset when reading the next response"))
	}
}

func TestTopicReaderCloseFromACKObserver(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	reader := newMetricsReader(&e)
	closed := make(chan error, 1)
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderCommittedNotify: func(trace.TopicReaderCommittedNotifyInfo) {
			closed <- reader.Close(e.ctx)
		},
	}
	e.reader.startTrace()
	e.Start()
	e.SendFromServer(readerMetricCommitResponse(&e, 21))
	require.NoError(t, xtest.Receive(t, closed, "Reader.Close from ACK observer"))
}

func TestTopicReaderFinalCommitAcknowledgement(t *testing.T) {
	for _, path := range []string{"reader close", "graceful partition stop"} {
		t.Run(path, func(t *testing.T) {
			e := newTopicReaderTestEnv(t)
			e.reader.committer.BufferTimeLagTrigger = time.Hour // flushed only by close or graceful stop
			received := make(chan int64, 1)
			e.reader.cfg.Trace = &trace.Topic{
				OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
				OnReaderCommittedNotify: func(info trace.TopicReaderCommittedNotifyInfo) {
					received <- info.MessagesCount
				},
			}
			e.reader.startTrace()
			e.partitionSession.SetupCommitMetrics(e.reader.cfg.Trace, e.reader.cfg.ReaderInfo)
			require.NoError(t, e.reader.startBackgroundWorkers())
			reader := newMetricsReader(&e)
			message := topicreadercommon.NewPublicMessageBuilder().PartitionSession(e.partitionSession).Offset(20).Build()
			require.NoError(t, reader.Commit(e.ctx, message))

			var count int64
			deliverAck := func(rawtopicreader.ClientMessage) error {
				select {
				case e.messagesFromServerToClient <- testStreamResult{msg: readerMetricCommitResponse(&e, 21)}:
					count = xtest.Receive(t, received, "final commit acknowledgement")
				case <-time.After(time.Second):
					t.Error("receive loop did not accept final ACK")
				}

				return nil
			}
			sent := e.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.CommitOffsetRequest{}))
			if path == "reader close" {
				sent.DoAndReturn(deliverAck)
				require.NoError(t, reader.Close(e.ctx))
			} else {
				sent.Return(nil)
				e.stream.EXPECT().Send(gomock.AssignableToTypeOf(&rawtopicreader.StopPartitionSessionResponse{})).
					After(sent.Call).DoAndReturn(deliverAck)
				require.NoError(t, e.reader.onStopPartitionSessionRequestFromBuffer(&rawtopicreader.StopPartitionSessionRequest{
					PartitionSessionID: e.partitionSessionID, Graceful: true,
				}))
			}
			require.Equal(t, int64(21), e.partitionSession.CommittedOffset().ToInt64())
			require.Equal(t, int64(1), count)
		})
	}
}
