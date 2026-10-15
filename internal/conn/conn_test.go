package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpc_testing "google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/stats"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ grpc.ClientConnInterface = (*connMock)(nil)

type connMock struct {
	cc grpc.ClientConnInterface
}

func (c connMock) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	_, _, err := invoke(ctx, method, args, reply, c.cc, "", 0, opts...)

	return err
}

func (c connMock) NewStream(
	ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.cc.NewStream(ctx, desc, method, opts...)
}

func TestConn(t *testing.T) {
	t.Run("Invoke", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			cc := mock.NewMockClientConnInterface(ctrl)
			cc.EXPECT().Invoke(
				gomock.Any(),
				Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
				&Ydb_Discovery.WhoAmIRequest{},
				&Ydb_Discovery.WhoAmIResponse{},
			).DoAndReturn(func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
				res, ok := reply.(*Ydb_Discovery.WhoAmIResponse)
				if !ok {
					return fmt.Errorf("reply is not *Ydb_Discovery.WhoAmIResponse: %T", reply)
				}

				res.Operation = &Ydb_Operations.Operation{
					Ready:  true,
					Status: Ydb.StatusIds_SUCCESS,
				}

				return nil
			})
			client := Ydb_Discovery_V1.NewDiscoveryServiceClient(&connMock{
				cc,
			})
			response, err := client.WhoAmI(ctx, &Ydb_Discovery.WhoAmIRequest{})
			require.NoError(t, err)
			require.NotNil(t, response)
		})
		t.Run("TransportError", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			cc := mock.NewMockClientConnInterface(ctrl)
			expectedErr := grpcStatus.Error(grpcCodes.Unavailable, "")
			cc.EXPECT().Invoke(
				gomock.Any(),
				Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
				&Ydb_Discovery.WhoAmIRequest{},
				&Ydb_Discovery.WhoAmIResponse{},
			).Return(expectedErr)
			client := Ydb_Discovery_V1.NewDiscoveryServiceClient(&connMock{
				cc,
			})
			response, err := client.WhoAmI(ctx, &Ydb_Discovery.WhoAmIRequest{})
			require.Error(t, err)
			require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			require.Nil(t, response)
		})
		t.Run("ContextCanceledReturnsContextError", func(t *testing.T) {
			ctx, cancel := context.WithCancel(xtest.Context(t))
			cancel()

			ctrl := gomock.NewController(t)
			cc := mock.NewMockClientConnInterface(ctrl)
			cc.EXPECT().Invoke(
				gomock.Any(),
				Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
				&Ydb_Discovery.WhoAmIRequest{},
				&Ydb_Discovery.WhoAmIResponse{},
			).Return(grpcStatus.Error(grpcCodes.Canceled, "rpc canceled"))

			client := Ydb_Discovery_V1.NewDiscoveryServiceClient(&connMock{
				cc,
			})
			response, err := client.WhoAmI(ctx, &Ydb_Discovery.WhoAmIRequest{})
			require.Error(t, err)
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, xerrors.IsTransportError(err, grpcCodes.Canceled))
			require.Nil(t, response)
		})
		t.Run("OperationError", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			t.Run("discovery.WhoAmI", func(t *testing.T) {
				cc := mock.NewMockClientConnInterface(ctrl)
				cc.EXPECT().Invoke(
					gomock.Any(),
					Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
					&Ydb_Discovery.WhoAmIRequest{},
					&Ydb_Discovery.WhoAmIResponse{},
				).DoAndReturn(func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
					res, ok := reply.(*Ydb_Discovery.WhoAmIResponse)
					if !ok {
						return fmt.Errorf("reply is not *Ydb_Discovery.WhoAmIResponse: %T", reply)
					}

					res.Operation = &Ydb_Operations.Operation{
						Ready:  true,
						Status: Ydb.StatusIds_UNAVAILABLE,
					}

					return nil
				})
				client := Ydb_Discovery_V1.NewDiscoveryServiceClient(&connMock{
					cc,
				})
				response, err := client.WhoAmI(ctx, &Ydb_Discovery.WhoAmIRequest{})
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
				require.Nil(t, response)
			})
			t.Run("query.BeginTransaction", func(t *testing.T) {
				cc := mock.NewMockClientConnInterface(ctrl)
				cc.EXPECT().Invoke(
					gomock.Any(),
					Ydb_Query_V1.QueryService_BeginTransaction_FullMethodName,
					&Ydb_Query.BeginTransactionRequest{},
					&Ydb_Query.BeginTransactionResponse{},
				).DoAndReturn(func(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
					res, ok := reply.(*Ydb_Query.BeginTransactionResponse)
					if !ok {
						return fmt.Errorf("reply is not *Ydb_Query.BeginTransactionResponse: %T", reply)
					}

					res.Status = Ydb.StatusIds_UNAVAILABLE

					return nil
				})
				client := Ydb_Query_V1.NewQueryServiceClient(&connMock{
					cc,
				})
				response, err := client.BeginTransaction(ctx, &Ydb_Query.BeginTransactionRequest{})
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
				require.Nil(t, response)
			})
		})
	})
}

func TestModificationMark(t *testing.T) {
	t.Run("NewMarkCanRetry", func(t *testing.T) {
		mark := &modificationMark{}
		require.True(t, mark.canRetry())
	})

	t.Run("DirtyMarkCannotRetry", func(t *testing.T) {
		mark := &modificationMark{}
		mark.markDirty()
		require.False(t, mark.canRetry())
	})

	t.Run("MarkDirtyMultipleTimes", func(t *testing.T) {
		mark := &modificationMark{}
		mark.markDirty()
		mark.markDirty()
		require.False(t, mark.canRetry())
	})
}

func TestMarkContext(t *testing.T) {
	t.Run("MarkContextCreatesNewMark", func(t *testing.T) {
		ctx := context.Background()
		newCtx, mark := markContext(ctx)
		require.NotNil(t, newCtx)
		require.NotNil(t, mark)
		require.True(t, mark.canRetry())
	})

	t.Run("GetContextMarkFromMarkedContext", func(t *testing.T) {
		ctx := context.Background()
		ctx, mark := markContext(ctx)
		retrievedMark := getContextMark(ctx)
		require.NotNil(t, retrievedMark)
		require.Equal(t, mark, retrievedMark)
	})

	t.Run("GetContextMarkFromUnmarkedContext", func(t *testing.T) {
		ctx := context.Background()
		mark := getContextMark(ctx)
		require.NotNil(t, mark)
		require.True(t, mark.canRetry())
	})

	t.Run("MarkFromContextReflectsDirtyState", func(t *testing.T) {
		ctx := context.Background()
		ctx, mark := markContext(ctx)
		mark.markDirty()
		retrievedMark := getContextMark(ctx)
		require.False(t, retrievedMark.canRetry())
	})
}

func TestReplyWrapper(t *testing.T) {
	t.Run("OperationResponse", func(t *testing.T) {
		resp := &Ydb_Discovery.WhoAmIResponse{
			Operation: &Ydb_Operations.Operation{
				Id:     "test-op-id",
				Ready:  true,
				Status: Ydb.StatusIds_SUCCESS,
			},
		}

		opID, issues := replyWrapper(resp)
		require.Equal(t, "test-op-id", opID)
		require.Empty(t, issues)
	})

	t.Run("StatusResponse", func(t *testing.T) {
		resp := &Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}

		opID, issues := replyWrapper(resp)
		require.Empty(t, opID)
		require.Empty(t, issues)
	})

	t.Run("NonOperationResponse", func(t *testing.T) {
		resp := &Ydb_Discovery.WhoAmIRequest{}

		opID, issues := replyWrapper(resp)
		require.Empty(t, opID)
		require.Empty(t, issues)
	})
}

func TestConn_StateManagement(t *testing.T) {
	t.Run("NewConnHasCreatedState", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		c := newConn(e, config)
		require.Equal(t, state.Created, c.State())
	})

	t.Run("EndpointMethodsWork", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135", endpoint.WithID(123))
		c := newConn(e, config)
		require.Equal(t, "test-endpoint:2135", c.Address())
		require.Equal(t, uint32(123), c.NodeID())
		require.NotNil(t, c.Endpoint())
	})

	t.Run("NilConnHandling", func(t *testing.T) {
		var c *conn
		require.Equal(t, uint32(0), c.NodeID())
		require.Nil(t, c.Endpoint())
	})
}

func TestConn_StaleClosedCheckMustNotRedial(t *testing.T) {
	ctx := t.Context()
	c := newConn(
		endpoint.New("passthrough:///127.0.0.1:1"),
		&mockConfig{
			grpcDialOpts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			},
		},
	)

	// Reproduce the two phases of realConn around c.mtx.Lock: its first
	// closed check succeeds, then Close wins the race before dialing starts.
	require.False(t, c.isClosed())
	require.NoError(t, c.Close(ctx))

	c.mtx.Lock()
	cc, err := c.dial(ctx)
	c.mtx.Unlock()
	if cc != nil {
		t.Cleanup(func() { _ = cc.Close() })
	}

	require.ErrorIs(t, err, errClosedConnection)
	require.Nil(t, cc)
	require.Nil(t, c.grpcConn)
}

func TestStatsHandler(t *testing.T) {
	t.Run("TagRPC", func(t *testing.T) {
		handler := statsHandler{}
		ctx := context.Background()
		newCtx := handler.TagRPC(ctx, &stats.RPCTagInfo{})
		require.Equal(t, ctx, newCtx)
	})

	t.Run("TagConn", func(t *testing.T) {
		handler := statsHandler{}
		ctx := context.Background()
		newCtx := handler.TagConn(ctx, &stats.ConnTagInfo{})
		require.Equal(t, ctx, newCtx)
	})

	t.Run("HandleConn", func(t *testing.T) {
		handler := statsHandler{}
		// Should not panic
		handler.HandleConn(context.Background(), &stats.ConnBegin{})
	})

	t.Run("HandleRPC_Begin", func(t *testing.T) {
		handler := statsHandler{}
		ctx, mark := markContext(context.Background())
		require.True(t, mark.canRetry())
		handler.HandleRPC(ctx, &stats.Begin{})
		// Begin should not mark as dirty
		require.True(t, mark.canRetry())
	})

	t.Run("HandleRPC_End", func(t *testing.T) {
		handler := statsHandler{}
		ctx, mark := markContext(context.Background())
		require.True(t, mark.canRetry())
		handler.HandleRPC(ctx, &stats.End{})
		// End should not mark as dirty
		require.True(t, mark.canRetry())
	})

	t.Run("HandleRPC_Other", func(t *testing.T) {
		handler := statsHandler{}
		ctx, mark := markContext(context.Background())
		require.True(t, mark.canRetry())
		handler.HandleRPC(ctx, &stats.InPayload{})
		// Other stats should mark as dirty
		require.False(t, mark.canRetry())
	})
}

func TestConn_OnClose(t *testing.T) {
	t.Run("OnCloseCalledOnClose", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")
		called := false
		onClose := func(c *conn) {
			called = true
		}
		c := newConn(e, config, withOnClose(onClose))
		err := c.Close(context.Background())
		require.NoError(t, err)
		require.True(t, called)
	})

	t.Run("MultipleOnCloseCalled", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")

		called1 := false
		called2 := false

		c := newConn(e, config,
			withOnClose(func(c *conn) { called1 = true }),
			withOnClose(func(c *conn) { called2 = true }),
		)

		err := c.Close(context.Background())
		require.NoError(t, err)
		require.True(t, called1)
		require.True(t, called2)
	})

	t.Run("OnCloseWithNilCallback", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout: 5 * time.Second,
		}
		e := endpoint.New("test-endpoint:2135")

		c := newConn(e, config, withOnClose(nil))

		err := c.Close(context.Background())
		require.NoError(t, err)
	})
}

func TestIsAvailable(t *testing.T) {
	t.Run("NilConnIsNotAvailable", func(t *testing.T) {
		require.False(t, isAvailable(nil))
	})
}

func TestConnInvokeCountsUnfinishedRPCs(t *testing.T) {
	for _, parking := range []bool{false, true} {
		t.Run(fmt.Sprint(parking), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c := newConn(endpoint.New("test"), &mockConfig{},
				withUsageTracker(clockwork.NewFakeClock(), parking))
			load := requireInFlightCounter(t, c)
			entered := make(chan struct{}, 3)
			release := make(chan struct{})
			c.grpcConn = &inFlightTransport{invoke: func(ctx context.Context) error {
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			results := make(chan error, 3)
			for range 3 {
				go func() { results <- c.Invoke(WithoutWrapping(ctx), "/test", nil, nil) }()
			}
			for range 3 {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("RPC did not enter transport")
				}
			}
			require.EqualValues(t, 3, load.InFlight())
			close(release)
			for range 3 {
				require.NoError(t, <-results)
			}
			require.Zero(t, load.InFlight())
		})
	}
}

func TestConnInFlightIsSharedByPoolUsers(t *testing.T) {
	ctx := WithoutWrapping(t.Context())
	pool := NewPool(ctx, &mockConfig{})
	t.Cleanup(func() { require.NoError(t, pool.RemoveRef(context.Background())) })
	e := endpoint.New("test")
	first := pool.Get(e)
	second := pool.Get(e)
	require.Same(t, first, second)
	load := requireInFlightCounter(t, first.(*conn))
	first.(*conn).grpcConn = &inFlightTransport{invoke: func(context.Context) error {
		require.EqualValues(t, 1, load.InFlight())

		return nil
	}}
	require.NoError(t, second.Invoke(ctx, "/test", nil, nil))
	require.Zero(t, load.InFlight())
	pool.Put(ctx, first)
	pool.Put(ctx, second)
}

func TestConnInvokeReleasesLoadOnErrors(t *testing.T) {
	for _, testErr := range []error{errors.New("transport failed"), context.Canceled} {
		t.Run(testErr.Error(), func(t *testing.T) {
			c := newConn(endpoint.New("test"), &mockConfig{})
			load := requireInFlightCounter(t, c)
			c.grpcConn = &inFlightTransport{invoke: func(context.Context) error {
				require.EqualValues(t, 1, load.InFlight())

				return testErr
			}}
			require.ErrorIs(t, c.Invoke(WithoutWrapping(t.Context()), "/test", nil, nil), testErr)
			require.Zero(t, load.InFlight())
		})
	}
}

func TestConnCountsRPCWhileLazyDialIsWaiting(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{}, 1)
			c := newConn(endpoint.New("passthrough:///test"), &mockConfig{
				grpcDialOpts: []grpc.DialOption{
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithBlock(), //nolint:staticcheck
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
						select {
						case entered <- struct{}{}:
						default:
						}
						<-ctx.Done()

						return nil, ctx.Err()
					}),
				},
			})
			load := requireInFlightCounter(t, c)
			result := make(chan error, 1)
			go func() {
				if stream {
					_, err := c.NewStream(ctx, &grpc.StreamDesc{}, "/test")
					result <- err
				} else {
					result <- c.Invoke(ctx, "/test", nil, nil)
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("dial did not start")
			}
			require.EqualValues(t, 1, load.InFlight())
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
			require.Zero(t, load.InFlight())
		})
	}
}

func TestConnStreamCreationErrorReleasesLoadOnce(t *testing.T) {
	for _, callback := range []bool{false, true} {
		t.Run(fmt.Sprint(callback), func(t *testing.T) {
			c := newConn(endpoint.New("test"), &mockConfig{})
			load := requireInFlightCounter(t, c)
			c.grpcConn = &inFlightTransport{newStream: func(opts []grpc.CallOption) (grpc.ClientStream, error) {
				require.EqualValues(t, 1, load.InFlight())
				if callback {
					for _, opt := range opts {
						if finish, ok := opt.(grpc.OnFinishCallOption); ok {
							finish.OnFinish(context.Canceled)
							finish.OnFinish(context.Canceled)
						}
					}
				}

				return nil, context.Canceled
			}}
			_, err := c.NewStream(t.Context(), &grpc.StreamDesc{}, "/test")
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, load.InFlight())
		})
	}
}

func TestConnStreamRemainsInflightUntilRPCFinishes(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			service := &inFlightTestService{halfClosed: make(chan struct{}), release: make(chan struct{})}
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			grpc_testing.RegisterTestServiceServer(server, service)
			t.Cleanup(server.Stop)
			t.Cleanup(func() { _ = listener.Close() })
			go func() { _ = server.Serve(listener) }()
			raw, err := grpc.NewClient("passthrough:///test",
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = raw.Close() })
			finished := make(chan struct{})
			c := newConn(endpoint.New("test"), &mockConfig{driverTrace: &trace.Driver{
				OnConnStreamFinish: func(trace.DriverConnStreamFinishInfo) { close(finished) },
			}})
			c.grpcConn = raw
			load := requireInFlightCounter(t, c)
			streamCtx, stopStream := context.WithCancel(ctx)
			defer stopStream()
			stream, err := grpc_testing.NewTestServiceClient(c).FullDuplexCall(streamCtx)
			require.NoError(t, err)
			require.EqualValues(t, 1, load.InFlight())
			require.NoError(t, stream.Send(&grpc_testing.StreamingOutputCallRequest{}))
			_, err = stream.Recv()
			require.NoError(t, err)
			require.EqualValues(t, 1, load.InFlight())
			require.NoError(t, stream.CloseSend())
			select {
			case <-service.halfClosed:
			case <-ctx.Done():
				t.Fatal("server did not observe half-close")
			}
			require.EqualValues(t, 1, load.InFlight())
			if canceled {
				stopStream()
			} else {
				close(service.release)
			}
			_, err = stream.Recv()
			if canceled {
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Canceled))
			} else {
				require.ErrorIs(t, err, io.EOF)
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("stream did not finish")
			}
			require.Zero(t, load.InFlight())
		})
	}
}

func TestConnDialFailureReleasesLoad(t *testing.T) {
	c := newConn(endpoint.New("test"), &mockConfig{})
	load := requireInFlightCounter(t, c)
	require.Error(t, c.Invoke(t.Context(), "/test", nil, nil))
	require.Zero(t, load.InFlight())
	_, err := c.NewStream(t.Context(), &grpc.StreamDesc{}, "/test")
	require.Error(t, err)
	require.Zero(t, load.InFlight())
}

// cpu: Apple M3 Pro; go1.27.0 darwin/arm64; GOMAXPROCS=4.
// RPC counters on master 75fb33113; median of five runs, one second per case.
//
//	GOTOOLCHAIN=go1.27.0 go test -run '^$' -bench '^BenchmarkConnInvoke$' \
//	  -benchmem -benchtime=1s -count=5 -cpu=4 ./internal/conn
//
// BenchmarkConnInvoke/Serial-4                    477.700 ns/op  1204 B/op  20 allocs/op
// BenchmarkConnInvoke/Parallel-4                  422.700 ns/op  1204 B/op  20 allocs/op
//
// Mock transport measures SDK wrapper cost, not network RPC latency.
func BenchmarkConnInvoke(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "Serial"
		if parallel {
			name = "Parallel"
		}
		b.Run(name, func(b *testing.B) {
			ctx := WithoutWrapping(b.Context())
			c := newConn(endpoint.New("test"), &mockConfig{})
			c.grpcConn = &mockGrpcConn{}
			b.ReportAllocs()
			b.ResetTimer()
			if parallel {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if err := c.Invoke(ctx, "/test", nil, nil); err != nil {
							b.Error(err)
						}
					}
				})
			} else {
				for range b.N {
					if err := c.Invoke(ctx, "/test", nil, nil); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func requireInFlightCounter(t testing.TB, c *conn) interface{ InFlight() int64 } {
	t.Helper()
	counter, ok := any(c).(interface{ InFlight() int64 })
	require.True(t, ok, "connection must expose unfinished logical RPC count")

	return counter
}

type inFlightTransport struct {
	mockGrpcConn

	invoke    func(context.Context) error
	newStream func([]grpc.CallOption) (grpc.ClientStream, error)
}

func (c *inFlightTransport) Invoke(ctx context.Context, _ string, _, _ any, _ ...grpc.CallOption) error {
	return c.invoke(ctx)
}

func (c *inFlightTransport) NewStream(
	_ context.Context, _ *grpc.StreamDesc, _ string, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.newStream(opts)
}

type inFlightTestService struct {
	grpc_testing.UnimplementedTestServiceServer

	halfClosed chan struct{}
	release    chan struct{}
}

func (s *inFlightTestService) FullDuplexCall(stream grpc_testing.TestService_FullDuplexCallServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err = stream.Send(&grpc_testing.StreamingOutputCallResponse{}); err != nil {
			return err
		}
	}
	close(s.halfClosed)
	select {
	case <-s.release:
		return nil
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}
