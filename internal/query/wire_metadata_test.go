package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/protobuf/proto"
)

func TestDecodeWirePartWithoutResultSet(t *testing.T) {
	want := &Ydb_Query.ExecuteQueryResponsePart{
		Status:    Ydb.StatusIds_SUCCESS,
		TxMeta:    &Ydb_Query.TransactionMeta{Id: "tx"},
		ExecStats: &Ydb_TableStats.QueryStats{TotalDurationUs: 123},
	}
	frame, err := proto.Marshal(want)
	require.NoError(t, err)
	part, err := decodeWirePart(frame)
	require.NoError(t, err)
	require.Nil(t, part.Meta().GetResultSet())
	require.Zero(t, part.RowCount())
	require.True(t, proto.Equal(want, part.Meta()), "transaction metadata and statistics must survive decoding")
}

func TestPrefetchedWireRecvPreservesInterceptorMetadata(t *testing.T) {
	original := &Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{{Name: "id", Type: wirePrimitive(Ydb.Type_UINT64)}},
			Rows:    []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}}}}},
		},
		ExecStats: &Ydb_TableStats.QueryStats{TotalDurationUs: 123},
	}
	frame, err := proto.Marshal(original)
	require.NoError(t, err)
	wire, err := decodeWirePart(frame)
	require.NoError(t, err)
	// Interceptors may replace metadata after the codec returns.
	wire.Meta().ExecStats = &Ydb_TableStats.QueryStats{TotalDurationUs: 456}
	wire.Meta().ResultSet.Columns[0].Name = "renamed"
	wire.Meta().ResultSet.Truncated = true
	stream := &asyncPrefetchExecuteQueryStream{ch: make(chan executeQueryPartRecv, 1)}
	stream.ch <- executeQueryPartRecv{part: wire.Meta(), wire: wire}
	part, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(456), part.GetExecStats().GetTotalDurationUs())
	require.Equal(t, "renamed", part.GetResultSet().GetColumns()[0].GetName())
	require.True(t, part.GetResultSet().GetTruncated())
	require.Len(t, part.GetResultSet().GetRows(), 1)
	require.Equal(t, uint64(42), part.GetResultSet().GetRows()[0].GetItems()[0].GetUint64Value())
	require.Empty(t, wire.Meta().GetResultSet().GetRows(), "typed Recv must not mutate retained wire metadata")
}
