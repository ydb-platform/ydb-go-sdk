package topicreaderinternal

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReader_MessagesDeliveredTrace(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	cancelledBatch := newTestReaderBatch(t, "/local/topic", 2)
	topicreadercommon.BatchGetPartitionSession(cancelledBatch).Close()
	deliveredBatch := newTestReaderBatch(t, "/local/topic", 3)
	singleMessageBatch := newTestReaderBatch(t, "/local/topic", 1)
	testErr := errors.New("test read error")

	baseReader := NewMockbatchedStreamReader(mc)
	gomock.InOrder(
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), ReadMessageBatchOptions{}).
			Return(cancelledBatch, nil),
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), ReadMessageBatchOptions{}).
			Return(deliveredBatch, nil),
		baseReader.EXPECT().ReadMessageBatch(
			gomock.Any(),
			ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MinCount: 1, MaxCount: 1}},
		).Return(singleMessageBatch, nil),
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), ReadMessageBatchOptions{}).
			Return(nil, testErr),
	)

	var events []trace.TopicReaderMessagesDeliveredInfo
	reader := &Reader{
		reader: baseReader,
		tracer: &trace.Topic{OnReaderMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			events = append(events, info)
		}},
		readerInfo: topicreadercommon.ReaderInfo{
			Endpoint: "configured:2135",
			Database: "/local",
			Consumer: "consumer",
		},
	}

	batch, err := reader.ReadMessageBatch(context.Background())
	require.NoError(t, err)
	require.Same(t, deliveredBatch, batch)
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Context)
	require.Equal(t, "configured:2135", events[0].Endpoint)
	require.Equal(t, "/local", events[0].Database)
	require.Equal(t, "/local/topic", events[0].Topic)
	require.Equal(t, "consumer", events[0].Consumer)
	require.Equal(t, 3, events[0].MessagesCount)

	message, err := reader.ReadMessage(context.Background())
	require.NoError(t, err)
	require.Same(t, singleMessageBatch.Messages[0], message)
	require.Len(t, events, 2)
	require.Equal(t, 1, events[1].MessagesCount)

	_, err = reader.ReadMessageBatch(context.Background())
	require.ErrorIs(t, err, testErr)
	require.Len(t, events, 2)
}

func TestReader_PopBatchTxMessagesDeliveredTrace(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	transaction := newMockTransactionWrapper("session", "transaction")
	deliveredBatch := newTestReaderBatch(t, "/local/topic", 2)
	testErr := errors.New("test transaction error")
	baseReader := NewMockbatchedStreamReader(mc)
	gomock.InOrder(
		baseReader.EXPECT().PopMessagesBatchTx(gomock.Any(), transaction, ReadMessageBatchOptions{}).
			Return(deliveredBatch, nil),
		baseReader.EXPECT().PopMessagesBatchTx(gomock.Any(), transaction, ReadMessageBatchOptions{}).
			Return(nil, testErr),
	)

	var events []trace.TopicReaderMessagesDeliveredInfo
	reader := &Reader{
		reader: baseReader,
		tracer: &trace.Topic{OnReaderMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			events = append(events, info)
		}},
		readerInfo: topicreadercommon.ReaderInfo{
			Endpoint: "configured:2135",
			Database: "/local",
			Consumer: "consumer",
		},
	}

	batch, err := reader.PopBatchTx(context.Background(), transaction)
	require.NoError(t, err)
	require.Same(t, deliveredBatch, batch)
	require.Len(t, events, 1)
	require.Equal(t, 2, events[0].MessagesCount)

	_, err = reader.PopBatchTx(context.Background(), transaction)
	require.ErrorIs(t, err, testErr)
	require.Len(t, events, 1)
}

func TestReader_EmptyBatchDoesNotTraceMessagesDelivered(t *testing.T) {
	var events []trace.TopicReaderMessagesDeliveredInfo
	reader := &Reader{
		tracer: &trace.Topic{OnReaderMessagesDelivered: func(info trace.TopicReaderMessagesDeliveredInfo) {
			events = append(events, info)
		}},
	}

	reader.traceMessagesDelivered(context.Background(), nil)
	reader.traceMessagesDelivered(context.Background(), &topicreadercommon.PublicBatch{})

	require.Empty(t, events)
}

func TestReader_Close(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		testErr := errors.New("test error")
		readerContext, readerCancel := xcontext.WithCancel(context.Background())
		baseReader := NewMockbatchedStreamReader(mc)
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), ReadMessageBatchOptions{}).
			DoAndReturn(func(ctx context.Context, options ReadMessageBatchOptions) (*topicreadercommon.PublicBatch, error) {
				<-readerContext.Done()

				return nil, testErr
			})
		baseReader.EXPECT().ReadMessageBatch(
			gomock.Any(),
			ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 1, MinCount: 1}},
		).DoAndReturn(func(ctx context.Context, options ReadMessageBatchOptions) (*topicreadercommon.PublicBatch, error) {
			<-readerContext.Done()

			return nil, testErr
		})
		baseReader.EXPECT().Commit(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitRange topicreadercommon.CommitRange) error {
				<-readerContext.Done()

				return testErr
			})
		baseReader.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ error) error {
			readerCancel()

			return nil
		})

		reader := &Reader{
			reader: baseReader,
		}

		type callState struct {
			callCompleted empty.Chan
			err           error
		}

		isCallCompleted := func(state *callState) bool {
			select {
			case <-state.callCompleted:
				return true
			default:
				return false
			}
		}

		var allStates []*callState
		newCallState := func() *callState {
			state := &callState{
				callCompleted: make(empty.Chan),
			}
			allStates = append(allStates, state)

			return state
		}

		readerCommitState := newCallState()
		readerReadMessageState := newCallState()
		readerReadMessageBatchState := newCallState()

		go func() {
			readerCommitState.err = reader.Commit(
				context.Background(),
				topicreadercommon.MessageWithSetCommitRangeForTest(
					&topicreadercommon.PublicMessage{},
					topicreadercommon.CommitRange{
						PartitionSession: &topicreadercommon.PartitionSession{},
					},
				),
			)
			close(readerCommitState.callCompleted)
		}()

		go func() {
			_, readerReadMessageState.err = reader.ReadMessage(context.Background())
			close(readerReadMessageState.callCompleted)
		}()

		go func() {
			_, readerReadMessageBatchState.err = reader.ReadMessageBatch(context.Background())
			close(readerReadMessageBatchState.callCompleted)
		}()

		runtime.Gosched()

		// check about no methods finished before close
		for i := range allStates {
			require.False(t, isCallCompleted(allStates[i]))
		}
		require.NoError(t, reader.Close(context.Background()))

		// check about all methods stop work after close
		for i := range allStates {
			<-allStates[i].callCompleted
			require.Error(t, allStates[i].err, i)
		}
	})
}

func TestReader_Commit(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		readerID := topicreadercommon.NextReaderID()
		baseReader := NewMockbatchedStreamReader(mc)
		reader := &Reader{
			reader:   baseReader,
			readerID: readerID,
		}

		expectedRangeOk := topicreadercommon.CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   10,
			PartitionSession:  newTestPartitionSessionReaderID(readerID, 10),
		}
		baseReader.EXPECT().Commit(gomock.Any(), expectedRangeOk).Return(nil)
		require.NoError(t, reader.Commit(
			context.Background(),
			topicreadercommon.MessageWithSetCommitRangeForTest(&topicreadercommon.PublicMessage{}, expectedRangeOk),
		))

		expectedRangeErr := topicreadercommon.CommitRange{
			CommitOffsetStart: 15,
			CommitOffsetEnd:   20,
			PartitionSession:  newTestPartitionSessionReaderID(readerID, 30),
		}

		testErr := errors.New("test err")
		baseReader.EXPECT().Commit(gomock.Any(), expectedRangeErr).Return(testErr)
		require.ErrorIs(t, reader.Commit(
			context.Background(),
			topicreadercommon.MessageWithSetCommitRangeForTest(
				&topicreadercommon.PublicMessage{},
				expectedRangeErr,
			),
		), testErr)
	})

	t.Run("CommitFromOtherReader", func(t *testing.T) {
		ctx := xtest.Context(t)
		reader := &Reader{readerID: 1}
		forCommit := topicreadercommon.CommitRange{
			CommitOffsetStart: 1,
			CommitOffsetEnd:   2,
			PartitionSession:  newTestPartitionSessionReaderID(2, 0),
		}
		err := reader.Commit(ctx, forCommit)
		require.ErrorIs(t, err, errCommitSessionFromOtherReader)
	})
}

func TestReader_WaitInit(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	readerID := topicreadercommon.NextReaderID()
	baseReader := NewMockbatchedStreamReader(mc)
	reader := &Reader{
		reader:   baseReader,
		readerID: readerID,
	}

	baseReader.EXPECT().WaitInit(gomock.Any())
	err := reader.WaitInit(context.Background())
	require.NoError(t, err)
}

func newTestPartitionSessionReaderID(
	readerID int64,
	partitionSessionID rawtopicreader.PartitionSessionID,
) *topicreadercommon.PartitionSession {
	return topicreadercommon.NewPartitionSession(
		context.Background(),
		"",
		0,
		readerID,
		"",
		partitionSessionID,
		int64(partitionSessionID+100),
		0,
	)
}

func newTestReaderBatch(t *testing.T, topic string, messagesCount int) *topicreadercommon.PublicBatch {
	t.Helper()

	session := topicreadercommon.NewPartitionSession(
		context.Background(),
		topic,
		1,
		1,
		"connection",
		1,
		1,
		0,
	)
	messages := make([]*topicreadercommon.PublicMessage, messagesCount)
	for i := range messages {
		messages[i] = &topicreadercommon.PublicMessage{}
	}
	batch, err := topicreadercommon.NewBatch(session, messages)
	require.NoError(t, err)

	return batch
}
