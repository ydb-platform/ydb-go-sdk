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
			recvMsgCounter atomic.Uint32
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
		require.Equal(t, uint32(1), recvMsgCounter.Load())
		<-startRecv
		<-stopRecv
		require.Equal(t, uint32(2), recvMsgCounter.Load())
		<-startRecv
		core.closeOnce()
		require.GreaterOrEqual(t, recvMsgCounter.Load(), uint32(2))
		require.LessOrEqual(t, recvMsgCounter.Load(), uint32(3))
		require.Equal(t, core.Status(), StatusClosed.String())
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreAttachError(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		var sessionDeletes atomic.Int32
		done := make(chan struct{})
		sessionDeletes.Store(0)
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				context.Context,
				*Ydb_Query.DeleteSessionRequest,
				...grpc.CallOption,
			) (*Ydb_Query.DeleteSessionResponse, error) {
				sessionDeletes.Add(1)

				return &Ydb_Query.DeleteSessionResponse{}, nil
			})
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			return nil, errSessionClosed
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		core, err := Open(ctx, client, func(core *sessionCore) {
			core.done = done
		})
		require.Error(t, err, errSessionClosed)
		require.Nil(t, core)
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreClose(t *testing.T) {
	debug.SetTraceback("all")
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
			unblock        atomic.Bool
			sessionDeletes atomic.Uint32
		)
		unblock.Store(false)
		sessionDeletes.Store(0)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			startRecv <- struct{}{}
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
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				context.Context,
				*Ydb_Query.DeleteSessionRequest,
				...grpc.CallOption,
			) (*Ydb_Query.DeleteSessionResponse, error) {
				if sessionDeletes.CompareAndSwap(0, 1) {
					return &Ydb_Query.DeleteSessionResponse{
						Status: Ydb.StatusIds_SUCCESS,
					}, nil
				}
				sessionDeletes.Add(1)

				return nil, errors.New("session not found")
			}).AnyTimes()
		core, err := Open(ctx, client, func(core *sessionCore) {
			done = core.done
		})
		require.NoError(t, err)
		require.NotNil(t, core)
		<-stopRecv

		var wg sync.WaitGroup
		parallel := runtime.GOMAXPROCS(0)
		if parallel > 10 {
			parallel = 10
		}
		for i := 0; i < parallel; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for {
					if unblock.Load() {
						_ = core.Close(ctx)

						break
					}
				}
			}(i)
		}
		unblock.Store(true)
		wg.Wait()
		_, ok := <-done
		require.False(t, ok)
		require.GreaterOrEqual(t, sessionDeletes.Load(), uint32(1))
		require.LessOrEqual(t, sessionDeletes.Load(), uint32(10))
	}, xtest.StopAfter(time.Second))
}
