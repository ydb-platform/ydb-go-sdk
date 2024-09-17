package conn

import (
	"context"
	"fmt"
	"testing"

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
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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
