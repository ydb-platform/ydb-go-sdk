package query

import (
	"context"
	"errors"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestSessionCoreCancelAttachOnDone(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}.Build(), nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		var (
			corePtr        atomic.Pointer[sessionCore]
			startRecv      = make(chan struct{}, 1)
			stopRecv       = make(chan struct{}, 1)
			recvMsgCounter atomic.Uint32
		)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			startRecv <- struct{}{}
			recvMsgCounter.Add(1)
			if c := corePtr.Load(); c != nil && c.closed.Load() {
				return nil, errSessionClosed
			}
			stopRecv <- struct{}{}

			return Ydb_Query.SessionState_builder{
				Status: Ydb.StatusIds_SUCCESS,
			}.Build(), nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
			SessionId: "123",
		}.Build()).Return(attachStream, nil)
		core, err := Open(ctx, client)
		require.NoError(t, err)
		require.NotNil(t, core)
		corePtr.Store(core)
		<-stopRecv
		require.Equal(t, uint32(1), recvMsgCounter.Load())
		<-startRecv
		<-stopRecv
		require.Equal(t, uint32(2), recvMsgCounter.Load())
		<-startRecv
		core.releaseSession()
		require.GreaterOrEqual(t, recvMsgCounter.Load(), uint32(2))
		require.LessOrEqual(t, recvMsgCounter.Load(), uint32(3))
		require.Equal(t, core.Status(), StatusClosed.String())
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreAttachError(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}.Build(), nil)
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *Ydb_Query.DeleteSessionRequest, _ ...grpc.CallOption) (
				*Ydb_Query.DeleteSessionResponse, error,
			) {
				return &Ydb_Query.DeleteSessionResponse{}, nil
			})
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			return nil, errSessionClosed
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
			SessionId: "123",
		}.Build()).Return(attachStream, nil)
		core, err := Open(ctx, client)
		require.ErrorIs(t, err, errSessionClosed)
		require.Nil(t, core)
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreClose(t *testing.T) {
	debug.SetTraceback("all")
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(Ydb_Query.CreateSessionResponse_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}.Build(), nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		var (
			corePtr        atomic.Pointer[sessionCore]
			startRecv      = make(chan struct{}, 1)
			stopRecv       = make(chan struct{}, 1)
			unblock        atomic.Bool
			sessionDeletes atomic.Uint32
		)
		unblock.Store(false)
		sessionDeletes.Store(0)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			select {
			case startRecv <- struct{}{}:
			case <-t.Context().Done():
				return nil, t.Context().Err()
			}

			if c := corePtr.Load(); c != nil && c.closed.Load() {
				return nil, errSessionClosed
			}

			select {
			case stopRecv <- struct{}{}:
			case <-t.Context().Done():
				return nil, t.Context().Err()
			}

			return Ydb_Query.SessionState_builder{
				Status: Ydb.StatusIds_SUCCESS,
			}.Build(), nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), Ydb_Query.AttachSessionRequest_builder{
			SessionId: "123",
		}.Build()).Return(attachStream, nil)
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *Ydb_Query.DeleteSessionRequest, _ ...grpc.CallOption) (
				*Ydb_Query.DeleteSessionResponse, error,
			) {
				if sessionDeletes.CompareAndSwap(0, 1) {
					return Ydb_Query.DeleteSessionResponse_builder{
						Status: Ydb.StatusIds_SUCCESS,
					}.Build(), nil
				}
				sessionDeletes.Add(1)

				return nil, errors.New("session not found")
			}).AnyTimes()
		core, err := Open(ctx, client)
		require.NoError(t, err)
		require.NotNil(t, core)
		corePtr.Store(core)
		<-stopRecv

		var wg sync.WaitGroup
		parallel := min(runtime.GOMAXPROCS(0), 10)
		for range parallel {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if unblock.Load() {
						_ = core.Close(ctx)

						break
					}
				}
			}()
		}
		unblock.Store(true)
		wg.Wait()
		require.True(t, core.closed.Load())
		require.GreaterOrEqual(t, sessionDeletes.Load(), uint32(1))
		require.LessOrEqual(t, sessionDeletes.Load(), uint32(10))
	}, xtest.StopAfter(time.Second))
}
