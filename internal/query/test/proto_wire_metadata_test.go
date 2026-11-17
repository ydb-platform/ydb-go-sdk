package test

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryProtoWireMetadataOnlyParts(t *testing.T) {
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			for _, withRows := range []bool{false, true} {
				t.Run(fmt.Sprintf("wire=%v/prefetch=%d/rows=%v", wire, prefetch, withRows), func(t *testing.T) {
					var parts []*Ydb_Query.ExecuteQueryResponsePart
					if withRows {
						parts = append(parts, &Ydb_Query.ExecuteQueryResponsePart{
							Status: Ydb.StatusIds_SUCCESS,
							ResultSet: &Ydb.ResultSet{
								Columns: []*Ydb.Column{{Name: "id", Type: &Ydb.Type{
									Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64},
								}}},
								Rows: []*Ydb.Value{{Items: []*Ydb.Value{
									{Value: &Ydb.Value_Uint64Value{Uint64Value: 42}},
								}}},
							},
						})
					}
					parts = append(parts,
						&Ydb_Query.ExecuteQueryResponsePart{
							Status: Ydb.StatusIds_SUCCESS,
							TxMeta: &Ydb_Query.TransactionMeta{Id: "tx"},
						},
						&Ydb_Query.ExecuteQueryResponsePart{
							Status:    Ydb.StatusIds_SUCCESS,
							ExecStats: &Ydb_TableStats.QueryStats{TotalDurationUs: 123},
						},
					)
					server := mock.Server(t, mock.WithExecuteQueryParts(parts...))
					db, err := ydb.Open(t.Context(), server.ConnString(),
						ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(wire))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })

					var gotStats query.Stats
					result, err := db.Query().Query(t.Context(), "SELECT id", query.WithResponsePartPrefetch(prefetch),
						query.WithStatsMode(query.StatsModeBasic, func(s query.Stats) { gotStats = s }))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, result.Close(t.Context())) })
					rs, err := result.NextResultSet(t.Context())
					require.NoError(t, err)
					if withRows {
						row, err := rs.NextRow(t.Context())
						require.NoError(t, err)
						var got uint64
						require.NoError(t, row.Scan(&got))
						require.Equal(t, uint64(42), got)
					}
					_, err = rs.NextRow(t.Context())
					require.ErrorIs(t, err, io.EOF, "metadata-only parts must not produce rows")
					_, err = result.NextResultSet(t.Context())
					require.ErrorIs(t, err, io.EOF)
					require.NotNil(t, gotStats, "trailing execution statistics must reach the caller")
					require.Equal(t, 123*time.Microsecond, gotStats.TotalDuration())
				})
			}
		}
	}
}
