package query

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	publicquery "github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestClientRowDecoding(t *testing.T) {
	conn := newWireResultConn(t, &wireResultServer{})
	for _, tt := range []struct {
		name string
		opts []config.Option
		wire bool
	}{
		{name: "default"},
		{name: "enabled", opts: []config.Option{config.WithProtoWireDecoding(true)}, wire: true},
		{name: "disabled", opts: []config.Option{config.WithProtoWireDecoding(false)}},
		{
			name: "last option wins",
			opts: []config.Option{config.WithProtoWireDecoding(true), config.WithProtoWireDecoding(false)},
		},
		{name: "implicit default", opts: []config.Option{config.AllowImplicitSessions()}},
		{
			name: "implicit enabled",
			opts: []config.Option{config.AllowImplicitSessions(), config.WithProtoWireDecoding(true)},
			wire: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			client, err := New(ctx, conn, config.New(tt.opts...))
			require.NoError(t, err)
			defer client.Close(ctx)

			for _, prefetch := range []int{0, 2} {
				t.Run(fmt.Sprintf("prefetch=%d", prefetch), func(t *testing.T) {
					r, err := client.Query(ctx, "SELECT id FROM test", options.WithResponsePartPrefetch(prefetch))
					require.NoError(t, err)
					defer r.Close(ctx)
					rs, err := r.NextResultSet(ctx)
					require.NoError(t, err)
					for _, want := range []uint64{42, 43} {
						row, err := rs.NextRow(ctx)
						require.NoError(t, err)
						var got uint64
						require.NoError(t, row.Scan(&got))
						require.Equal(t, want, got)
						require.Equal(t, tt.wire, row.(*Row).part != nil, "row storage must match the selected decoder")
					}
					_, err = rs.NextRow(ctx)
					require.ErrorIs(t, err, io.EOF)
				})
			}
		})
	}
}

func TestExecuteQueryUsesWireRowsAcrossParts(t *testing.T) {
	conn := newWireResultConn(t, &wireResultServer{})

	for _, prefetch := range []int{0, 2} {
		t.Run(fmt.Sprintf("prefetch=%d", prefetch), func(t *testing.T) {
			ctx := t.Context()
			session := newTestSessionWithClient("session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)), false)
			r, err := session.Query(ctx, "SELECT id FROM test", options.WithResponsePartPrefetch(prefetch))
			require.NoError(t, err)
			defer r.Close(ctx)

			rs, err := r.NextResultSet(ctx)
			require.NoError(t, err)
			for _, want := range []uint64{42, 43} {
				row, err := rs.NextRow(ctx)
				require.NoError(t, err)
				require.IsType(t, &Row{}, row)
				var got uint64
				require.NoError(t, row.Scan(&got))
				require.Equal(t, want, got)
			}
			_, err = rs.NextRow(ctx)
			require.ErrorIs(t, err, io.EOF)
		})
	}
	for _, prefetch := range []int{0, 2} {
		t.Run(fmt.Sprintf("typed recv prefetch=%d", prefetch), func(t *testing.T) {
			client := WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn))
			stream, err := client.ExecuteQuery(t.Context(), &Ydb_Query.ExecuteQueryRequest{})
			require.NoError(t, err)
			stream = wrapExecuteQueryStreamWithAsyncPrefetch(stream, prefetch)
			part, err := stream.Recv()
			require.NoError(t, err)
			require.Len(t, part.GetResultSet().GetRows(), 1)
			require.Equal(t, uint64(42), part.GetResultSet().GetRows()[0].GetItems()[0].GetUint64Value())
		})
	}
	t.Run("materialized", func(t *testing.T) {
		ctx := t.Context()
		r, err := execute(ctx, "session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)), "SELECT id FROM test",
			options.ExecuteSettings(), options.ResultSetsTypeOrdered)
		require.NoError(t, err)
		materialized, err := resultToMaterializedResult(ctx, r)
		require.NoError(t, err)
		require.NoError(t, r.Close(ctx))
		rs, err := materialized.NextResultSet(ctx)
		require.NoError(t, err)
		for _, want := range []uint64{42, 43} {
			row, err := rs.NextRow(ctx)
			require.NoError(t, err)
			var got uint64
			require.NoError(t, row.Scan(&got))
			require.Equal(t, want, got)
		}
	})
	t.Run("single row", func(t *testing.T) {
		ctx := t.Context()
		r, err := execute(ctx, "session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)),
			"SELECT id FROM test LIMIT 1", options.ExecuteSettings(), options.ResultSetsTypeOrdered)
		require.NoError(t, err)
		row, err := readRow(ctx, r)
		require.NoError(t, err)
		var got uint64
		require.NoError(t, row.Scan(&got))
		require.Equal(t, uint64(42), got)
	})
	t.Run("multiple result sets", func(t *testing.T) {
		ctx := t.Context()
		r, err := execute(ctx, "session", WireQueryClient(Ydb_Query_V1.NewQueryServiceClient(conn)), "MULTI",
			options.ExecuteSettings(options.WithResponsePartPrefetch(2)), options.ResultSetsTypeOrdered)
		require.NoError(t, err)
		defer r.Close(ctx)
		for setIndex, ids := range [][]uint64{{42, 43}, {44}} {
			rs, err := r.NextResultSet(ctx)
			require.NoError(t, err)
			require.Equal(t, setIndex, rs.Index())
			for _, want := range ids {
				row, err := rs.NextRow(ctx)
				require.NoError(t, err)
				var got uint64
				require.NoError(t, row.Scan(&got))
				require.Equal(t, want, got)
			}
			_, err = rs.NextRow(ctx)
			require.ErrorIs(t, err, io.EOF)
		}
		_, err = r.NextResultSet(ctx)
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestQueryPreservesScannedOptionalValues(t *testing.T) {
	server := &optionalWireResultServer{
		columns: []*Ydb.Column{
			{Name: "score", Type: wireOptional(Ydb.Type_INT32)},
			{Name: "active", Type: wireOptional(Ydb.Type_BOOL)},
			{Name: "amount", Type: wireOptional(Ydb.Type_DOUBLE)},
			{Name: "name", Type: wireOptional(Ydb.Type_UTF8)},
			{Name: "payload", Type: wireOptional(Ydb.Type_STRING)},
		},
	}
	for i, word := range []string{"one", "two"} {
		server.rows = append(server.rows, &Ydb.Value{Items: []*Ydb.Value{
			{Value: &Ydb.Value_Int32Value{Int32Value: int32(i + 1)}},
			{Value: &Ydb.Value_BoolValue{BoolValue: i == 0}},
			{Value: &Ydb.Value_DoubleValue{DoubleValue: float64(i) + 1.5}},
			{Value: &Ydb.Value_TextValue{TextValue: word}},
			{Value: &Ydb.Value_BytesValue{BytesValue: []byte(word)}},
		}})
	}
	conn := newWireResultConn(t, server)
	type destination struct {
		Score   *int32   `sql:"score"`
		Active  *bool    `sql:"active"`
		Amount  *float64 `sql:"amount"`
		Name    *string  `sql:"name"`
		Payload *[]byte  `sql:"payload"`
	}
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			for _, tt := range []struct {
				name string
				scan func(publicquery.Row, *destination) error
			}{
				{name: "Scan", scan: func(row publicquery.Row, dst *destination) error {
					return row.Scan(&dst.Score, &dst.Active, &dst.Amount, &dst.Name, &dst.Payload)
				}},
				{name: "ScanNamed", scan: func(row publicquery.Row, dst *destination) error {
					return row.ScanNamed(scanner.NamedRef("score", &dst.Score), scanner.NamedRef("active", &dst.Active),
						scanner.NamedRef("amount", &dst.Amount), scanner.NamedRef("name", &dst.Name),
						scanner.NamedRef("payload", &dst.Payload))
				}},
				{name: "ScanStruct", scan: func(row publicquery.Row, dst *destination) error {
					return row.ScanStruct(dst)
				}},
			} {
				t.Run(fmt.Sprintf("wire=%v/prefetch=%d/%s", wire, prefetch, tt.name), func(t *testing.T) {
					ctx := t.Context()
					client, err := New(ctx, conn, config.New(config.WithProtoWireDecoding(wire)))
					require.NoError(t, err)
					defer client.Close(ctx)

					r, err := client.Query(ctx, "SELECT score, active, amount, name, payload FROM test",
						options.WithResponsePartPrefetch(prefetch))
					require.NoError(t, err)
					defer r.Close(ctx)
					rs, err := r.NextResultSet(ctx)
					require.NoError(t, err)
					var dst destination
					var saved []destination
					for range server.rows {
						row, err := rs.NextRow(ctx)
						require.NoError(t, err)
						require.NoError(t, tt.scan(row, &dst))
						saved = append(saved, dst)
					}
					_, err = rs.NextRow(ctx)
					require.ErrorIs(t, err, io.EOF)
					var got [][]any
					for _, item := range saved {
						got = append(got, []any{*item.Score, *item.Active, *item.Amount, *item.Name, *item.Payload})
					}
					require.Equal(t, [][]any{
						{int32(1), true, 1.5, "one", []byte("one")},
						{int32(2), false, 2.5, "two", []byte("two")},
					}, got, "scanning the second row must preserve the first row's values")
				})
			}
		}
	}
}

func newWireResultConn(t *testing.T, queryServer Ydb_Query_V1.QueryServiceServer) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	Ydb_Query_V1.RegisterQueryServiceServer(server, queryServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

type wireResultServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer
}

type optionalWireResultServer struct {
	wireResultServer

	columns []*Ydb.Column
	rows    []*Ydb.Value
}

func (s *optionalWireResultServer) ExecuteQuery(_ *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	for i, row := range s.rows {
		part := &Ydb_Query.ExecuteQueryResponsePart{
			Status:    Ydb.StatusIds_SUCCESS,
			ResultSet: &Ydb.ResultSet{Rows: []*Ydb.Value{row}},
		}
		if i == 0 {
			part.ResultSet.Columns = s.columns
		}
		if err := stream.Send(part); err != nil {
			return err
		}
	}

	return nil
}

func (*wireResultServer) CreateSession(context.Context, *Ydb_Query.CreateSessionRequest) (
	*Ydb_Query.CreateSessionResponse, error,
) {
	return &Ydb_Query.CreateSessionResponse{Status: Ydb.StatusIds_SUCCESS, SessionId: "session"}, nil
}

func (*wireResultServer) AttachSession(_ *Ydb_Query.AttachSessionRequest,
	stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	if err := stream.Send(&Ydb_Query.SessionState{Status: Ydb.StatusIds_SUCCESS}); err != nil {
		return err
	}
	<-stream.Context().Done()

	return stream.Context().Err()
}

func (*wireResultServer) DeleteSession(context.Context, *Ydb_Query.DeleteSessionRequest) (
	*Ydb_Query.DeleteSessionResponse, error,
) {
	return &Ydb_Query.DeleteSessionResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (*wireResultServer) ExecuteQuery(request *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	columns := []*Ydb.Column{{Name: "id", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}}}}
	ids := []uint64{42, 43}
	if strings.Contains(request.GetQueryContent().GetText(), "LIMIT 1") {
		ids = ids[:1]
	}
	for i, id := range ids {
		part := &Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			ResultSet: &Ydb.ResultSet{Rows: []*Ydb.Value{{Items: []*Ydb.Value{
				{Value: &Ydb.Value_Uint64Value{Uint64Value: id}},
			}}}},
		}
		if i == 0 {
			part.ResultSet.Columns = columns
		}
		if err := stream.Send(part); err != nil {
			return err
		}
	}
	if strings.Contains(request.GetQueryContent().GetText(), "MULTI") {
		return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 1,
			ResultSet: &Ydb.ResultSet{
				Columns: columns,
				Rows:    []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Uint64Value{Uint64Value: 44}}}}},
			},
		})
	}

	return nil
}
