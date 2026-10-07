package wirevalue

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestWireValueCodecReceivesQueryPart(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	Ydb_Query_V1.RegisterQueryServiceServer(server, &wireQueryServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	stream, err := Ydb_Query_V1.NewQueryServiceClient(conn).ExecuteQuery(context.Background(),
		&Ydb_Query.ExecuteQueryRequest{}, grpc.ForceCodecV2(NewCodec()))
	require.NoError(t, err)

	var part Part
	require.NoError(t, stream.RecvMsg(&part))
	require.Equal(t, Ydb.StatusIds_SUCCESS, part.Meta().GetStatus())
	require.Len(t, part.rows, 1)
	var id uint64
	require.NoError(t, part.Row(0).Scan(&id))
	require.Equal(t, uint64(42), id)
}

type wireQueryServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer
}

func (*wireQueryServer) ExecuteQuery(_ *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}},
			Rows:    []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}}}}},
		},
	})
}
