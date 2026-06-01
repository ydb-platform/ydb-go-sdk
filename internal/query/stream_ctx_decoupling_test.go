package query

import (
	"context"
	"io"
	"sync/atomic"
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

// TestStreamResult_PerCallCtxCancelDoesNotRunExecuteOnCloseEarly ensures a cancelled per-call ctx
// does not poison lastErr and does not run executeCancel (shutdown hook) until Close.
func TestStreamResult_PerCallCtxCancelDoesNotRunExecuteOnCloseEarly(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, executeCancel := context.WithCancel(t.Context())
		stubExecuteQueryStreamContext(executeCtx, stream)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}, nil)

		var onCloseCalls atomic.Uint64
		onClose := func() {
			onCloseCalls.Add(1)
			executeCancel()
		}

		r, err := newResult(t.Context(), stream, nil, withStreamResultOnClose(onClose))
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err = r.nextResultSet(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 0, onCloseCalls.Load(),
			"per-call ctx cancel must not run executeCancel shutdown hook")
		require.NoError(t, r.lastErr)
	})
}

// TestStreamResult_CloseDrainsExecStatsAfterPerCallCtxCancel models late ExecStats after nextResultSet
// failed on a cancelled per-call ctx.
func TestStreamResult_CloseDrainsExecStatsAfterPerCallCtxCancel(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		execStats := &Ydb_TableStats.QueryStats{
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

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		_, opts := executeQueryStreamContextWithOnClose(stream)
		gomock.InOrder(
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet:      &Ydb.ResultSet{},
			}, nil),
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:    Ydb.StatusIds_SUCCESS,
				ExecStats: execStats,
			}, nil),
			stream.EXPECT().Recv().Return(nil, io.EOF),
		)

		var (
			onCloseCalls atomic.Uint64
			gotStats     atomic.Bool
		)

		r, err := newResult(t.Context(), stream, append(opts,
			withStreamResultOnClose(func() { onCloseCalls.Add(1) }),
			withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				if queryStats == nil {
					return
				}
				if _, ok := queryStats.NextPhase(); ok {
					gotStats.Store(true)
				}
			}),
		)...)
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err = r.nextResultSet(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 0, onCloseCalls.Load())
		require.False(t, gotStats.Load())

		require.NoError(t, r.Close(t.Context()))
		require.EqualValues(t, 1, onCloseCalls.Load())
		require.True(t, gotStats.Load(),
			"Close with fresh ctx must drain ExecStats after per-call ctx cancel")
	})
}

// TestStreamResult_CloseIsIdempotentViaStreamContext ensures repeated Close after drain
// is a no-op once shutdown hooks cancel the gRPC stream context.
func TestStreamResult_CloseIsIdempotentViaStreamContext(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		_, opts := executeQueryStreamContextWithOnClose(stream)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)

		r, err := newResult(t.Context(), stream, opts...)
		require.NoError(t, err)

		require.NoError(t, r.Close(t.Context()))
		require.NoError(t, r.Close(t.Context()))
	})
}

// TestStreamResult_CloseTimeoutInterruptsBlockedRecv ensures closeTimeout cancels the
// execute stream (via shutdown hooks) and unblocks a stalled drain Recv().
func TestStreamResult_CloseTimeoutInterruptsBlockedRecv(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctrl := gomock.NewController(t)

		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		executeCtx, opts := executeQueryStreamContextWithOnClose(stream)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}, nil)
		stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			<-executeCtx.Done()

			return nil, executeCtx.Err()
		})

		r, err := newResult(t.Context(), stream, append(opts,
			withStreamResultCloseTimeout(50*time.Millisecond),
		)...)
		require.NoError(t, err)

		start := time.Now()
		err = r.Close(t.Context())
		elapsed := time.Since(start)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second,
			"Close must return after closeTimeout, not hang on blocked Recv")
	})
}
