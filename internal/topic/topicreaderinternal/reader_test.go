package topicreaderinternal

import (
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestReader_Close(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		testErr := errors.New("test error")
		readerContext, readerCancel := xcontext.WithCancel(context.Background())
		baseReader := NewMockbatchedStreamReader(mc)
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), ReadMessageBatchOptions{}).Do(func(_, _ interface{}) {
			<-readerContext.Done()
		}).Return(nil, testErr)
		baseReader.EXPECT().ReadMessageBatch(
			gomock.Any(),
			ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 1, MinCount: 1}},
		).Do(func(_, _ interface{}) {
			<-readerContext.Done()
		}).Return(nil, testErr)
		baseReader.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(_, _ interface{}) {
			<-readerContext.Done()
		}).Return(testErr)
		baseReader.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Do(func(_, _ interface{}) {
			readerCancel()
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
			readerCommitState.err = reader.Commit(context.Background(), &PublicMessage{
				commitRange: commitRange{
					partitionSession: &partitionSession{},
				},
			})
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

		readerID := nextReaderID()
		baseReader := NewMockbatchedStreamReader(mc)
		reader := &Reader{
			reader:   baseReader,
			readerID: readerID,
		}

		expectedRangeOk := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   10,
			partitionSession: &partitionSession{
				readerID:           readerID,
				partitionSessionID: 10,
			},
		}
		baseReader.EXPECT().Commit(gomock.Any(), expectedRangeOk).Return(nil)
		require.NoError(t, reader.Commit(context.Background(), &PublicMessage{commitRange: expectedRangeOk}))

		expectedRangeErr := commitRange{
			commitOffsetStart: 15,
			commitOffsetEnd:   20,
			partitionSession: &partitionSession{
				readerID:           readerID,
				partitionSessionID: 30,
			},
		}

		testErr := errors.New("test err")
		baseReader.EXPECT().Commit(gomock.Any(), expectedRangeErr).Return(testErr)
		require.ErrorIs(t, reader.Commit(context.Background(), &PublicMessage{commitRange: expectedRangeErr}), testErr)
	})

	t.Run("CommitFromOtherReader", func(t *testing.T) {
		ctx := xtest.Context(t)
		reader := &Reader{readerID: 1}
		forCommit := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   2,
			partitionSession:  &partitionSession{readerID: 2},
		}
		err := reader.Commit(ctx, forCommit)
		require.ErrorIs(t, err, errCommitSessionFromOtherReader)
	})
}

func TestReader_WaitInit(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	readerID := nextReaderID()
	baseReader := NewMockbatchedStreamReader(mc)
	reader := &Reader{
		reader:   baseReader,
		readerID: readerID,
	}

	baseReader.EXPECT().WaitInit(gomock.Any())
	err := reader.WaitInit(context.Background())
	require.NoError(t, err)
}
