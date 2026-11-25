package query

import (
	"context"
	"reflect"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type wireQueryClient struct {
	Ydb_Query_V1.QueryServiceClient

	protobuf encoding.CodecV2
}

// WireQueryClient enables wire decoding of FORMAT_VALUE query results.
func WireQueryClient(client Ydb_Query_V1.QueryServiceClient) Ydb_Query_V1.QueryServiceClient {
	return newWireQueryClient(client, encoding.GetCodecV2("proto"))
}

func newWireQueryClient(
	client Ydb_Query_V1.QueryServiceClient, protobuf encoding.CodecV2,
) Ydb_Query_V1.QueryServiceClient {
	// gRPC exposes its built-in codec only through the registry. A V1 override
	// leaves the V2 slot empty, and a custom V2 can use the same "proto" name.
	t := reflect.TypeOf(protobuf)
	if t == nil || t.Kind() != reflect.Pointer || t.Elem().PkgPath() != "google.golang.org/grpc/encoding/proto" {
		return client
	}

	return wireQueryClient{QueryServiceClient: client, protobuf: protobuf}
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
	decoder := &codec{protobuf: c.protobuf}
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
	// A buffering interceptor may receive other responses while serving this
	// request. Only the destination passed to RecvMsg can use the wire view.
	var receive wireReceive
	s.decoder.capture.Store(&receive)
	defer s.decoder.capture.Store(nil)
	if err := s.RecvMsg(&receive.part); err != nil {
		return nil, nil, err
	}

	return &receive.part, receive.wire, nil
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
