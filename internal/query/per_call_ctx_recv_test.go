package query

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func testEmptyStreamPart() *Ydb_Query.ExecuteQueryResponsePart {
	return &Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet:      &Ydb.ResultSet{},
	}
}

func testSingleRowStreamPart() *Ydb_Query.ExecuteQueryResponsePart {
	return &Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{
				Name: "id",
				Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}},
			}},
			Rows: []*Ydb.Value{
				{Items: []*Ydb.Value{{Value: &Ydb.Value_Int64Value{Int64Value: 10}}}},
			},
		},
	}
}

// Per-call ctx cancellation is checked only before stream.Recv(). Once Recv blocks,
// canceling the iteration ctx does not cancel executeCtx; the user must Close the
// result (which runs executeCancel) to unblock the stream.

func TestPerCallCtx_CancelBeforeRecvReturnsImmediately(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, opts := executeQueryStreamContextWithOnClose(stream)
		stream.EXPECT().Recv().Return(testEmptyStreamPart(), nil)

		r, err := newResult(t.Context(), stream, opts...)
		require.NoError(t, err)

		callCtx, callCancel := context.WithCancel(t.Context())
		callCancel()

		_, err = r.nextPart(callCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.NoError(t, r.lastErr)
		require.NoError(t, executeCtx.Err())
	})
}

func TestPerCallCtx_DeadlineExceededBeforeRecvDoesNotPoisonLastErr(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, opts := executeQueryStreamContextWithOnClose(stream)
		stream.EXPECT().Recv().Return(testEmptyStreamPart(), nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		r, err := newResult(t.Context(), stream, opts...)
		require.NoError(t, err)

		callCtx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
		defer cancel()
		<-callCtx.Done()

		_, err = r.nextPart(callCtx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NoError(t, r.lastErr)
		require.NoError(t, executeCtx.Err())

		require.NoError(t, r.Close(t.Context()))
	})
}

func TestPerCallCtx_CancelWhileRecvBlockedDoesNotCancelExecuteStream(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, executeCancel := context.WithCancel(t.Context())
		stubExecuteQueryStreamContext(executeCtx, stream)

		recvEntered := make(chan struct{})
		gomock.InOrder(
			stream.EXPECT().Recv().Return(testEmptyStreamPart(), nil),
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				close(recvEntered)

				<-executeCtx.Done()

				return nil, executeCtx.Err()
			}),
		)

		r, err := newResult(t.Context(), stream, withStreamResultOnClose(executeCancel))
		require.NoError(t, err)

		callCtx, callCancel := context.WithCancel(t.Context())

		done := make(chan error, 1)
		go func() {
			_, err := r.nextPart(callCtx)
			done <- err
		}()

		<-recvEntered
		callCancel()

		require.ErrorIs(t, callCtx.Err(), context.Canceled)
		require.NoError(t, executeCtx.Err(),
			"per-call ctx cancel during blocked Recv must not cancel execute stream")

		select {
		case err := <-done:
			t.Fatalf("nextPart returned before execute stream shutdown: %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		executeCancel()

		select {
		case err := <-done:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("nextPart still blocked after executeCancel (Close shutdown hook)")
		}
		require.NoError(t, r.lastErr)
	})
}

func TestPerCallCtx_CloseUnblocksConcurrentBlockedNextPart(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, opts := executeQueryStreamContextWithOnClose(stream)

		recvEntered := make(chan struct{})
		gomock.InOrder(
			stream.EXPECT().Recv().Return(testEmptyStreamPart(), nil),
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				close(recvEntered)

				<-executeCtx.Done()

				return nil, executeCtx.Err()
			}),
			stream.EXPECT().Recv().Return(nil, io.EOF),
		)

		r, err := newResult(t.Context(), stream, append(opts,
			withStreamResultCloseTimeout(50*time.Millisecond),
		)...)
		require.NoError(t, err)

		callCtx, callCancel := context.WithCancel(t.Context())

		iterDone := make(chan error, 1)
		go func() {
			_, err := r.nextPart(callCtx)
			iterDone <- err
		}()

		<-recvEntered
		callCancel()
		require.NoError(t, executeCtx.Err())

		start := time.Now()
		closeErr := r.Close(t.Context())
		elapsed := time.Since(start)

		require.Less(t, elapsed, time.Second,
			"Close must unblock blocked Recv via onClose, not hang past closeTimeout")

		select {
		case err := <-iterDone:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("iteration goroutine still blocked after Close")
		}

		// Drain may finish with EOF or close-timeout depending on scheduling.
		if closeErr != nil {
			require.ErrorIs(t, closeErr, context.DeadlineExceeded)
		}
	})
}

func TestPerCallCtx_NextResultSetCancelWhileRecvBlocked(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, executeCancel := context.WithCancel(t.Context())
		stubExecuteQueryStreamContext(executeCtx, stream)

		part := testSingleRowStreamPart()
		recvEntered := make(chan struct{})

		gomock.InOrder(
			stream.EXPECT().Recv().Return(part, nil),
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				close(recvEntered)

				<-executeCtx.Done()

				return nil, executeCtx.Err()
			}),
			stream.EXPECT().Recv().Return(nil, io.EOF),
		)

		r, err := newResult(t.Context(), stream, append([]resultOption{
			withStreamResultOnClose(executeCancel),
		}, withStreamResultCloseTimeout(50*time.Millisecond))...)
		require.NoError(t, err)

		bg := t.Context()
		rs, err := r.NextResultSet(bg)
		require.NoError(t, err)
		_, err = rs.NextRow(bg)
		require.NoError(t, err)

		callCtx, callCancel := context.WithCancel(t.Context())

		iterDone := make(chan error, 1)
		go func() {
			_, err := r.NextResultSet(callCtx)
			iterDone <- err
		}()

		<-recvEntered
		callCancel()
		require.NoError(t, executeCtx.Err())

		select {
		case err := <-iterDone:
			t.Fatalf("NextResultSet returned before Close: %v", err)
		case <-time.After(200 * time.Millisecond):
		}

		require.NoError(t, r.Close(bg))

		select {
		case err := <-iterDone:
			require.Error(t, err)
		case <-time.After(time.Second):
			t.Fatal("NextResultSet still blocked after Close")
		}
	})
}

func TestPerCallCtx_CloseDrainsAfterBlockedIterationCanceled(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, opts := executeQueryStreamContextWithOnClose(stream)

		recvEntered := make(chan struct{})
		gomock.InOrder(
			stream.EXPECT().Recv().Return(testEmptyStreamPart(), nil),
			stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				close(recvEntered)

				<-executeCtx.Done()

				return nil, executeCtx.Err()
			}),
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:    Ydb.StatusIds_SUCCESS,
				ExecStats: docapiCommitExecStatsForTest(),
			}, nil),
			stream.EXPECT().Recv().Return(nil, io.EOF),
		)

		var gotStats bool
		r, err := newResult(t.Context(), stream, append(opts,
			withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				if queryStats != nil {
					gotStats = true
				}
			}),
		)...)
		require.NoError(t, err)

		callCtx, callCancel := context.WithCancel(t.Context())

		iterDone := make(chan struct{})
		go func() {
			_, _ = r.nextPart(callCtx)
			close(iterDone)
		}()

		<-recvEntered
		callCancel()

		require.NoError(t, r.Close(t.Context()))

		<-iterDone
		require.True(t, gotStats,
			"Close must drain late stream parts after iteration was stuck on blocked Recv")
	})
}

// docapiCommitExecStatsForTest mirrors mock/docapi stats shape used in commit regressions.
func docapiCommitExecStatsForTest() *Ydb_TableStats.QueryStats {
	return &Ydb_TableStats.QueryStats{
		QueryPhases: []*Ydb_TableStats.QueryPhaseStats{
			{
				TableAccess: []*Ydb_TableStats.TableAccessStats{
					{
						Name:    "table",
						Deletes: &Ydb_TableStats.OperationStats{Rows: 1, Bytes: 1},
					},
				},
			},
		},
	}
}
