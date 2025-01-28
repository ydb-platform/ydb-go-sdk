package query

import (
	"testing"
	"time"

	"github.com/streadway/handy/atomic"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestSessionCoreCancelAttachOnDone(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		var (
			done           chan struct{}
			startRecv      = make(chan struct{}, 1)
			stopRecv       = make(chan struct{}, 1)
			recvMsgCounter atomic.Int
		)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			startRecv <- struct{}{}
			recvMsgCounter.Add(1)
			select {
			case <-done:
				return nil, errSessionClosed
			case stopRecv <- struct{}{}:
				return &Ydb_Query.SessionState{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			}
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		core, err := Open(ctx, client, func(core *sessionCore) {
			done = core.done
		})
		require.NoError(t, err)
		require.NotNil(t, core)
		<-stopRecv
		require.Equal(t, int64(1), recvMsgCounter.Get())
		<-startRecv
		<-stopRecv
		require.Equal(t, int64(2), recvMsgCounter.Get())
		<-startRecv
		close(done)
		require.GreaterOrEqual(t, recvMsgCounter.Get(), int64(2))
		require.LessOrEqual(t, recvMsgCounter.Get(), int64(3))
	}, xtest.StopAfter(time.Second))
}
