package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
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
	if conn.HasCodecCallOption(opts...) {
		return c.QueryServiceClient.ExecuteQuery(ctx, request, opts...)
	}
	decoder := newWireCodec()
	stream, err := c.QueryServiceClient.ExecuteQuery(conn.WithFallbackCodec(ctx, decoder), request, opts...)
	if err != nil {
		return nil, err
	}

	return wireQueryStream{QueryService_ExecuteQueryClient: stream, decoder: decoder}, nil
}

type wireQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	decoder *codec
}

func (s wireQueryStream) RecvPart() (*Ydb_Query.ExecuteQueryResponsePart, *wirePart, error) {
	var wire *wirePart
	// RecvMsg and its interceptors keep their protobuf destination. The codec
	// captures wire rows only during this receive; typed Recv still decodes rows.
	// Like gRPC RecvMsg, RecvPart must not be called concurrently on one stream.
	s.decoder.capture = &wire
	defer func() { s.decoder.capture = nil }()
	part, err := s.QueryService_ExecuteQueryClient.Recv()
	if err != nil {
		return nil, nil, err
	}

	return part, wire, nil
}

// Recv returns the ordinary protobuf view. Wire consumers must use RecvPart.
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
