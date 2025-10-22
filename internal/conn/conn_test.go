package conn

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	"google.golang.org/grpc/stats"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

//go:generate mockgen -destination grpc_client_conn_interface_mock_test.go --typed -package conn -write_package_comment=false google.golang.org/grpc ClientConnInterface

var _ grpc.ClientConnInterface = (*connMock)(nil)

type connMock struct {
	cc grpc.ClientConnInterface
}

func (c connMock) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	_, _, err := invoke(ctx, method, args, reply, c.cc, nil, "", 0, opts...)

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
			cc := NewMockClientConnInterface(ctrl)
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
			cc := NewMockClientConnInterface(ctrl)
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
		t.Run("OperationError", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			t.Run("discovery.WhoAmI", func(t *testing.T) {
				cc := NewMockClientConnInterface(ctrl)
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
				cc := NewMockClientConnInterface(ctrl)
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
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		c := newConn(e, config)
		
		require.Equal(t, Created, c.GetState())
	})

	t.Run("IsStateChecksMultipleStates", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		c := newConn(e, config)
		
		c.setState(context.Background(), Online)
		
		require.True(t, c.IsState(Online))
		require.True(t, c.IsState(Online, Offline))
		require.False(t, c.IsState(Offline))
		require.False(t, c.IsState(Banned, Destroyed))
	})

	t.Run("EndpointMethodsWork", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
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
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		
		c := newConn(e, config, withOnClose(nil))
		
		err := c.Close(context.Background())
		require.NoError(t, err)
	})
}

func TestConn_OnTransportError(t *testing.T) {
	t.Run("OnTransportErrorCalledOnError", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		
		var capturedConn Conn
		var capturedErr error
		
		onTransportError := func(ctx context.Context, cc Conn, cause error) {
			capturedConn = cc
			capturedErr = cause
		}
		
		c := newConn(e, config, withOnTransportError(onTransportError))
		
		testErr := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test"))
		c.onTransportError(context.Background(), testErr)
		
		require.Equal(t, c, capturedConn)
		require.Equal(t, testErr, capturedErr)
	})

	t.Run("OnTransportErrorWithNilCallback", func(t *testing.T) {
		config := &mockConfig{
			dialTimeout:   5 * time.Second,
			connectionTTL: 0,
		}
		e := endpoint.New("test-endpoint:2135")
		
		c := newConn(e, config, withOnTransportError(nil))
		
		// Should not panic
		testErr := xerrors.Transport(grpcStatus.Error(grpcCodes.Unavailable, "test"))
		c.onTransportError(context.Background(), testErr)
	})
}

func TestIsAvailable(t *testing.T) {
	t.Run("NilConnIsNotAvailable", func(t *testing.T) {
		require.False(t, isAvailable(nil))
	})
}
