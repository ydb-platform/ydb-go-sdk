package query

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

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
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet:      &Ydb.ResultSet{},
		}, nil)

		var onCloseCalls atomic.Uint64
		onClose := func() {
			onCloseCalls.Add(1)
		}

		r, err := newResult(context.Background(), stream, withStreamResultOnClose(onClose))
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(context.Background())
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

		r, err := newResult(context.Background(), stream,
			withStreamResultOnClose(func() { onCloseCalls.Add(1) }),
			withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				if queryStats == nil {
					return
				}
				if _, ok := queryStats.NextPhase(); ok {
					gotStats.Store(true)
				}
			}),
		)
		require.NoError(t, err)

		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = r.nextResultSet(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 0, onCloseCalls.Load())
		require.False(t, gotStats.Load())

		require.NoError(t, r.Close(context.Background()))
		require.EqualValues(t, 1, onCloseCalls.Load())
		require.True(t, gotStats.Load(),
			"Close with fresh ctx must drain ExecStats after per-call ctx cancel")
	})
}
