package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
)

type wireQueryClient struct {
	Ydb_Query_V1.QueryServiceClient
}

// WireQueryClient enables wire decoding of FORMAT_VALUE query results.
func WireQueryClient(client Ydb_Query_V1.QueryServiceClient) Ydb_Query_V1.QueryServiceClient {
	return wireQueryClient{client}
}

func (c wireQueryClient) ExecuteQuery(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest,
	opts ...grpc.CallOption,
) (Ydb_Query_V1.QueryService_ExecuteQueryClient, error) {
	if request.GetResultSetFormat() == Ydb.ResultSet_FORMAT_ARROW {
		return c.QueryServiceClient.ExecuteQuery(ctx, request, opts...)
	}
	stream, err := c.QueryServiceClient.ExecuteQuery(ctx, request, append(opts, wireQueryCodecOption())...)
	if err != nil {
		return nil, err
	}

	return wireQueryStream{stream}, nil
}

type wireQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient
}

func (s wireQueryStream) RecvPart() (*Ydb_Query.ExecuteQueryResponsePart, *wirePart, error) {
	var part wirePart
	if err := s.QueryService_ExecuteQueryClient.RecvMsg(&part); err != nil {
		return nil, nil, err
	}

	return part.Meta(), &part, nil
}

func (s wireQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	return s.QueryService_ExecuteQueryClient.Recv()
}

func recvQueryPart(stream Ydb_Query_V1.QueryService_ExecuteQueryClient) (
	*Ydb_Query.ExecuteQueryResponsePart, *wirePart, error,
) {
	if s, ok := stream.(interface {
		RecvPart() (*Ydb_Query.ExecuteQueryResponsePart, *wirePart, error)
	}); ok {
		return s.RecvPart()
	}
	part, err := stream.Recv()

	return part, nil, err
}

func wireQueryCodecOption() grpc.CallOption {
	return grpc.ForceCodecV2(newWireCodec())
}
