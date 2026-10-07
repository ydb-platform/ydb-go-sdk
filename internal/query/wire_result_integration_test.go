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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
)

func TestExecuteQueryUsesWireRowsAcrossParts(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	Ydb_Query_V1.RegisterQueryServiceServer(server, &wireResultServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

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

type wireResultServer struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer
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
