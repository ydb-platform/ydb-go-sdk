package query

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
)

func TestWireQueryStreamAcceptsProtobufTransport(t *testing.T) {
	want := &Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{Rows: []*Ydb.Value{{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}},
		}}}},
	}
	client := WireQueryClient(codecQueryClient{stream: protobufQueryStream{
		codecQueryStream: &codecQueryStream{part: want},
	}})
	stream, err := client.ExecuteQuery(t.Context(), &Ydb_Query.ExecuteQueryRequest{})
	require.NoError(t, err)
	part, wire, err := recvQueryPart(stream)
	require.NoError(t, err)
	require.True(t, proto.Equal(want, part), "a transport that supplies protobuf messages must preserve its rows")
	require.Nil(t, wire, "a transport that bypasses the wire codec must use its protobuf rows")
}

func TestWireQueryStreamMixedReceives(t *testing.T) {
	server := &optionalWireResultServer{
		columns: []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}},
	}
	for _, id := range []uint64{42, 43, 44} {
		server.rows = append(server.rows, &Ydb.Value{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Uint64Value{Uint64Value: id}},
		}})
	}
	client := WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(newWireResultConn(t, server)))
	stream, err := client.ExecuteQuery(t.Context(), &Ydb_Query.ExecuteQueryRequest{})
	require.NoError(t, err)
	for i, want := range []uint64{42, 43, 44} {
		if i == 1 {
			part, err := stream.Recv()
			require.NoError(t, err)
			require.Len(t, part.GetResultSet().GetRows(), 1, "typed Recv must materialize protobuf rows")
			require.Equal(t, want, part.GetResultSet().GetRows()[0].GetItems()[0].GetUint64Value())

			continue
		}
		part, wire, err := recvQueryPart(stream)
		require.NoError(t, err)
		require.NotNil(t, wire)
		require.Same(t, part, wire.Meta(), "interceptors and wire rows must share response metadata")
		require.Empty(t, part.GetResultSet().GetRows(), "wire reads must avoid materializing protobuf rows")
		var got uint64
		require.NoError(t, wire.row(0, server.columns).Scan(&got))
		require.Equal(t, want, got)
	}
	part, wire, err := recvQueryPart(stream)
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, part)
	require.Nil(t, wire, "EOF must not return a previously captured response")
}

func TestWireQueryRespectsPerQueryCodec(t *testing.T) {
	conn := newWireResultConn(t, &wireResultServer{})
	codec := rewritingQueryCodec{CodecV2: encoding.GetCodecV2("proto"), id: 9001}
	for _, tt := range []struct {
		name string
		opts []grpc.CallOption
	}{
		{name: "ForceCodecV2", opts: []grpc.CallOption{grpc.ForceCodecV2(codec)}},
		{name: "ForceCodec", opts: []grpc.CallOption{grpc.ForceCodec(queryCodecV1{codec})}},
		{name: "CustomCodec", opts: []grpc.CallOption{
			grpc.CustomCodecCallOption{Codec: queryLegacyCodec{queryCodecV1{codec}}},
		}},
		{name: "last V1 wins", opts: []grpc.CallOption{
			grpc.ForceCodecV2(encoding.GetCodecV2("proto")), grpc.ForceCodec(queryCodecV1{codec}),
		}},
		{name: "last V2 wins", opts: []grpc.CallOption{
			grpc.ForceCodec(queryCodecV1{rewritingQueryCodec{CodecV2: encoding.GetCodecV2("proto"), id: 42}}),
			grpc.ForceCodecV2(codec),
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			session := newTestSessionWithClient("session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)), false)
			ctx := t.Context()
			result, err := session.Query(ctx, "SELECT id FROM test LIMIT 1", options.WithCallOptions(tt.opts...))
			require.NoError(t, err)
			defer result.Close(ctx)
			rs, err := result.NextResultSet(ctx)
			require.NoError(t, err)
			row, err := rs.NextRow(ctx)
			require.NoError(t, err)
			var got uint64
			require.NoError(t, row.Scan(&got))
			require.Equal(t, uint64(9001), got, "the per-query codec must determine the decoded result")
			require.Nil(t, row.(*Row).part, "custom codecs must use the ordinary protobuf row path")
		})
	}
}

func TestWireQueryExplicitCodecKeepsProtobufStream(t *testing.T) {
	codec := rewritingQueryCodec{CodecV2: encoding.GetCodecV2("proto"), id: 9001}
	for _, tt := range []struct {
		name string
		opt  grpc.CallOption
	}{
		{name: "V1", opt: grpc.ForceCodec(queryCodecV1{codec})},
		{name: "V1 pointer", opt: &grpc.ForceCodecCallOption{Codec: queryCodecV1{codec}}},
		{name: "V2", opt: grpc.ForceCodecV2(codec)},
		{name: "V2 pointer", opt: &grpc.ForceCodecV2CallOption{CodecV2: codec}},
		{name: "subtype", opt: grpc.CallContentSubtype("proto")},
		{name: "subtype pointer", opt: &grpc.ContentSubtypeCallOption{ContentSubtype: "proto"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			underlying := &codecQueryStream{part: &Ydb_Query.ExecuteQueryResponsePart{Status: Ydb.StatusIds_SUCCESS}}
			client := WireQueryClient(codecQueryClient{stream: underlying})
			stream, err := client.ExecuteQuery(t.Context(), &Ydb_Query.ExecuteQueryRequest{}, tt.opt)
			require.NoError(t, err)
			require.Equal(t, underlying, stream, "explicit codecs must preserve the protobuf receive contract")
			part, wire, err := recvQueryPart(stream)
			require.NoError(t, err)
			require.Same(t, underlying.part, part)
			require.Nil(t, wire)
		})
	}
}

func TestWireQueryRespectsRegisteredCodecSelection(t *testing.T) {
	encoding.RegisterCodecV2(registeredQueryCodec{
		rewritingQueryCodec: rewritingQueryCodec{CodecV2: encoding.GetCodecV2("proto"), id: 9002},
	})
	conn := newWireResultConn(t, &wireResultServer{})
	session := newTestSessionWithClient("session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)), false)
	ctx := t.Context()
	result, err := session.Query(ctx, "SELECT id FROM test LIMIT 1", options.WithCallOptions(
		grpc.CallContentSubtype("query-wire-review-test"),
	))
	require.NoError(t, err)
	defer result.Close(ctx)
	rs, err := result.NextResultSet(ctx)
	require.NoError(t, err)
	row, err := rs.NextRow(ctx)
	require.NoError(t, err)
	var got uint64
	require.NoError(t, row.Scan(&got))
	require.Equal(t, uint64(9002), got, "the registered per-query codec must determine the decoded result")
	require.Nil(t, row.(*Row).part)
}

type rewritingQueryCodec struct {
	encoding.CodecV2

	id uint64
}

type queryCodecV1 struct {
	rewritingQueryCodec
}

func (c queryCodecV1) Marshal(src any) ([]byte, error) {
	data, err := c.CodecV2.Marshal(src)
	if err != nil {
		return nil, err
	}
	defer data.Free()

	owned := make([]byte, data.Len())
	data.CopyTo(owned)

	return owned, nil
}

func (c queryCodecV1) Unmarshal(data []byte, dst any) error {
	return c.rewritingQueryCodec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, dst)
}

type registeredQueryCodec struct {
	rewritingQueryCodec
}

type queryLegacyCodec struct {
	queryCodecV1
}

func (c queryLegacyCodec) String() string { return c.Name() }

func (registeredQueryCodec) Name() string { return "query-wire-review-test" }

func (c rewritingQueryCodec) Unmarshal(data mem.BufferSlice, dst any) error {
	if err := c.CodecV2.Unmarshal(data, dst); err != nil {
		return err
	}
	if part, ok := dst.(*Ydb_Query.ExecuteQueryResponsePart); ok {
		part.GetResultSet().GetRows()[0].GetItems()[0].GetValue().(*Ydb.Value_Uint64Value).Uint64Value = c.id
	}

	return nil
}

type codecQueryClient struct {
	Ydb_Query_V1.QueryServiceClient

	stream Ydb_Query_V1.QueryService_ExecuteQueryClient
}

func (c codecQueryClient) ExecuteQuery(context.Context, *Ydb_Query.ExecuteQueryRequest, ...grpc.CallOption) (
	Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
) {
	return c.stream, nil
}

type codecQueryStream struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	part *Ydb_Query.ExecuteQueryResponsePart
}

type protobufQueryStream struct {
	*codecQueryStream
}

func (s protobufQueryStream) RecvMsg(message any) error {
	part, ok := message.(*Ydb_Query.ExecuteQueryResponsePart)
	if !ok {
		return fmt.Errorf("query transport: expected protobuf response, got %T", message)
	}
	proto.Merge(part, s.part)

	return nil
}

func (s protobufQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	part := new(Ydb_Query.ExecuteQueryResponsePart)
	if err := s.RecvMsg(part); err != nil {
		return nil, err
	}

	return part, nil
}

func (s *codecQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	return s.part, nil
}
