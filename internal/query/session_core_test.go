package query

import (
	"context"
	"errors"
	"io"
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
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSessionCoreCancelAttachOnDone(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
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

			return &Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
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
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *Ydb_Query.DeleteSessionRequest, _ ...grpc.CallOption) (
				*Ydb_Query.DeleteSessionResponse, error,
			) {
				return &Ydb_Query.DeleteSessionResponse{}, nil
			})
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			return nil, errSessionClosed
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		var closedEvents atomic.Uint32
		core, err := Open(ctx, client,
			WithPoolName("/local"),
			WithTrace(&trace.Query{
				OnSessionClosed: func(trace.QuerySessionClosedInfo) {
					closedEvents.Add(1)
				},
			}),
		)
		require.ErrorIs(t, err, errSessionClosed)
		require.Nil(t, core)
		require.Zero(t, closedEvents.Load(), "initial attach failure must not close a working session")
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreClosedReasons(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		msg    *Ydb_Query.SessionState
		err    error
	}{
		{
			name:   "AttachStreamClosed",
			reason: "attach_closed",
			err:    io.EOF,
		},
		{
			name:   "AttachStreamError",
			reason: "transport_error",
			err:    grpcStatus.Error(grpcCodes.Unavailable, "attach stream failed"),
		},
		{
			name:   "NodeShutdown",
			reason: "node_shutdown",
			msg: &Ydb_Query.SessionState{
				SessionHint: &Ydb_Query.SessionState_NodeShutdown{
					NodeShutdown: &Ydb_Query.NodeShutdownHint{},
				},
			},
		},
		{
			name:   "SessionShutdown",
			reason: "session_shutdown",
			msg: &Ydb_Query.SessionState{
				SessionHint: &Ydb_Query.SessionState_SessionShutdown{
					SessionShutdown: &Ydb_Query.SessionShutdownHint{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
				SessionId: "123",
			}).Return(&Ydb_Query.DeleteSessionResponse{}, nil)

			var events []trace.QuerySessionClosedInfo
			core := &sessionCore{
				Client: client,
				Trace: &trace.Query{
					OnSessionClosed: func(info trace.QuerySessionClosedInfo) {
						events = append(events, info)
					},
				},
				id:             "123",
				poolName:       "/local",
				onNodeShutdown: func(error) {},
			}
			core.status.Store(uint32(StatusIdle))

			attachStream := NewMockQueryService_AttachSessionClient(ctrl)
			attachStream.EXPECT().Recv().Return(test.msg, test.err)
			core.listenAttachStream(attachStream)
			require.NoError(t, core.Close(t.Context()))

			require.Equal(t, []trace.QuerySessionClosedInfo{{
				PoolName: "/local",
				Reason:   test.reason,
			}}, events)
		})
	}
}

func TestSessionCoreLocalCloseDoesNotReportClosedMetric(t *testing.T) {
	tests := []struct {
		name string
		msg  *Ydb_Query.SessionState
		err  error
	}{
		{
			name: "TransportError",
			err:  context.Canceled,
		},
		{
			name: "SessionShutdown",
			msg: &Ydb_Query.SessionState{
				SessionHint: &Ydb_Query.SessionState_SessionShutdown{
					SessionShutdown: &Ydb_Query.SessionShutdownHint{},
				},
			},
		},
		{
			name: "NodeShutdown",
			msg: &Ydb_Query.SessionState{
				SessionHint: &Ydb_Query.SessionState_NodeShutdown{
					NodeShutdown: &Ydb_Query.NodeShutdownHint{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			attachStream := NewMockQueryService_AttachSessionClient(ctrl)
			recvStarted := make(chan struct{})
			unblockRecv := make(chan struct{})
			attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
				close(recvStarted)
				<-unblockRecv

				return test.msg, test.err
			})

			var events atomic.Uint32
			core := &sessionCore{
				Trace: &trace.Query{
					OnSessionClosed: func(trace.QuerySessionClosedInfo) {
						events.Add(1)
					},
				},
				poolName:       "/local",
				onNodeShutdown: func(error) {},
			}
			core.status.Store(uint32(StatusIdle))

			done := make(chan struct{})
			go func() {
				defer close(done)
				core.listenAttachStream(attachStream)
			}()
			<-recvStarted
			core.closed.Store(true)
			close(unblockRecv)
			<-done

			require.Zero(t, events.Load())
		})
	}
}

func TestSessionCoreClosedMetricDisabledWithoutClientPool(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := NewMockQueryServiceClient(ctrl)
	client.EXPECT().DeleteSession(gomock.Any(), &Ydb_Query.DeleteSessionRequest{
		SessionId: "123",
	}).Return(&Ydb_Query.DeleteSessionResponse{}, nil)

	var events atomic.Uint32
	core := &sessionCore{
		Client: client,
		Trace: &trace.Query{
			OnSessionClosed: func(trace.QuerySessionClosedInfo) {
				events.Add(1)
			},
		},
		id: "123",
	}
	core.status.Store(uint32(StatusIdle))

	attachStream := NewMockQueryService_AttachSessionClient(ctrl)
	attachStream.EXPECT().Recv().Return(nil, io.EOF)
	core.listenAttachStream(attachStream)
	require.NoError(t, core.Close(t.Context()))
	require.Zero(t, events.Load())
}

func TestSessionCoreClose(t *testing.T) {
	debug.SetTraceback("all")
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)
		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
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

			return &Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)
		client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *Ydb_Query.DeleteSessionRequest, _ ...grpc.CallOption) (
				*Ydb_Query.DeleteSessionResponse, error,
			) {
				if sessionDeletes.CompareAndSwap(0, 1) {
					return &Ydb_Query.DeleteSessionResponse{
						Status: Ydb.StatusIds_SUCCESS,
					}, nil
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

func TestSessionCoreNodeShutdownHintBansConnection(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
			NodeId:    1,
		}, nil)

		var (
			firstRecv   atomic.Bool
			deliverHint = make(chan struct{})
			closeGate   sync.Once
		)
		t.Cleanup(func() {
			closeGate.Do(func() { close(deliverHint) })
		})

		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		var banned atomic.Bool
		ctx = conn.WithBanCallback(ctx, func(cause error) {
			banned.Store(true)
			require.ErrorIs(t, cause, errNodeShutdownHint)
		})
		stubAttachStreamContextWith(ctx, attachStream)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			if !firstRecv.Swap(true) {
				return &Ydb_Query.SessionState{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			}

			<-deliverHint

			return &Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
				SessionHint: &Ydb_Query.SessionState_NodeShutdown{
					NodeShutdown: &Ydb_Query.NodeShutdownHint{},
				},
			}, nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)

		core, err := Open(ctx, client)
		require.NoError(t, err)
		require.NotNil(t, core)

		closeGate.Do(func() { close(deliverHint) })

		require.Eventually(t, func() bool {
			return banned.Load() && !core.IsAlive()
		}, time.Second, time.Millisecond,
			"NodeShutdown hint must ban the connection and release the session",
		)
		require.Equal(t, StatusClosed.String(), core.Status())
	}, xtest.StopAfter(time.Second))
}

func TestSessionCoreSessionShutdownHintClosesSession(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := t.Context()

		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
			Status:    Ydb.StatusIds_SUCCESS,
			SessionId: "123",
		}, nil)

		var (
			firstRecv   atomic.Bool
			deliverHint = make(chan struct{})
			closeGate   sync.Once
		)
		t.Cleanup(func() {
			closeGate.Do(func() { close(deliverHint) })
		})

		attachStream := NewMockQueryService_AttachSessionClient(ctrl)
		stubAttachStreamContext(attachStream)
		attachStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.SessionState, error) {
			if !firstRecv.Swap(true) {
				return &Ydb_Query.SessionState{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			}

			<-deliverHint

			return &Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
				SessionHint: &Ydb_Query.SessionState_SessionShutdown{
					SessionShutdown: &Ydb_Query.SessionShutdownHint{},
				},
			}, nil
		}).AnyTimes()
		client.EXPECT().AttachSession(gomock.Any(), &Ydb_Query.AttachSessionRequest{
			SessionId: "123",
		}).Return(attachStream, nil)

		core, err := Open(ctx, client)
		require.NoError(t, err)
		require.NotNil(t, core)

		closeGate.Do(func() { close(deliverHint) })

		require.Eventually(t, func() bool {
			return !core.IsAlive()
		}, time.Second, time.Millisecond,
			"SessionShutdown hint must release the session",
		)
		require.Equal(t, StatusClosed.String(), core.Status())
	})
}
