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
