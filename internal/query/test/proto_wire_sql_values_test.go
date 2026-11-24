package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestSQLWireMismatchedScalarPayloadsMatchProtobuf(t *testing.T) {
	part := &Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{
				{Name: "active", Type: types.Bool.ToYDB()},
				{Name: "label", Type: types.Text.ToYDB()},
				{Name: "optional", Type: types.NewOptional(types.Bool).ToYDB()},
			},
			Rows: []*Ydb.Value{{Items: []*Ydb.Value{
				{Value: &Ydb.Value_Uint64Value{Uint64Value: 1}},
				{Value: &Ydb.Value_BytesValue{BytesValue: []byte("wrong arm")}},
				{Value: &Ydb.Value_Uint64Value{Uint64Value: 1}},
			}}},
		},
	}
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			t.Run(fmt.Sprintf("wire=%v/prefetch=%d", wire, prefetch), func(t *testing.T) {
				server := mock.Server(t, mock.WithExecuteQueryParts(part))
				observer := &queryDecoderObserver{}
				db, err := ydb.Open(t.Context(), server.ConnString(),
					ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(wire),
					ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(observer.intercept))),
				)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })
				connector, err := ydb.Connector(db, ydb.WithQueryService(true), ydb.WithPrefetchQueryResultParts(prefetch))
				require.NoError(t, err)
				sqlDB := sql.OpenDB(connector)
				t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
				observer.check(t, wire, func() {
					var active bool
					var label string
					var optional *bool
					err := sqlDB.QueryRowContext(t.Context(), "SELECT active, label, optional").Scan(&active, &label, &optional)
					require.NoError(t, err)
					require.False(t, active)
					require.Empty(t, label)
					require.Equal(t, new(bool), optional)
				})
			})
		}
	}
}

func TestSQLWireDecodingPreservesValuesAcrossParts(t *testing.T) {
	firstTime := time.Date(2026, time.November, 17, 12, 0, 0, 123000, time.UTC)
	lastTime := firstTime.Add(time.Second)
	want := []sqlWireValues{
		{
			id: 1, text: sqlWireRef("first"), number: sqlWireRef(1.25), timestamp: &firstTime,
			hash: sqlWireRef(uint64(1)<<63 + 1), bytes: []byte{0, 1, 2},
		},
		{id: 2},
		{
			id: 3, text: sqlWireRef("last"), number: sqlWireRef(-2.5), timestamp: &lastTime,
			hash: sqlWireRef(uint64(1)<<63 + 3), bytes: []byte{3, 4, 5},
		},
	}
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			t.Run(fmt.Sprintf("wire=%v/prefetch=%d", wire, prefetch), func(t *testing.T) {
				server := mock.Server(t, mock.WithExecuteQueryParts(sqlWireResultParts(want)...))
				observer := &queryDecoderObserver{}
				db, err := ydb.Open(t.Context(), server.ConnString(),
					ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(wire),
					ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(observer.intercept))),
				)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })
				connector, err := ydb.Connector(db, ydb.WithQueryService(true), ydb.WithPrefetchQueryResultParts(prefetch))
				require.NoError(t, err)
				sqlDB := sql.OpenDB(connector)
				t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

				observer.check(t, wire, func() {
					rows, err := sqlDB.QueryContext(t.Context(), "SELECT SLO payload")
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, rows.Close()) })
					columns, err := rows.ColumnTypes()
					require.NoError(t, err)
					require.Len(t, columns, 6)
					for i, column := range columns {
						require.Equal(t, sqlWireColumnNames[i], column.Name())
						require.Equal(t, sqlWireColumnTypes[i].Yql(), column.DatabaseTypeName())
						nullable, ok := column.Nullable()
						require.True(t, ok)
						require.Equal(t, i != 0, nullable)
					}
					var row sqlWireValues
					var saved []sqlWireValues
					for rows.Next() {
						require.NoError(t, rows.Scan(&row.id, &row.text, &row.number, &row.timestamp, &row.hash, &row.bytes))
						saved = append(saved, row)
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())
					require.Len(t, saved, len(want))
					for i, got := range saved {
						require.Equal(t, want[i].id, got.id)
						require.Equal(t, want[i].text, got.text)
						require.Equal(t, want[i].number, got.number)
						require.Equal(t, want[i].hash, got.hash)
						require.Equal(t, want[i].bytes, got.bytes)
						if want[i].timestamp == nil {
							require.Nil(t, got.timestamp)
						} else {
							require.NotNil(t, got.timestamp)
							require.WithinDuration(t, *want[i].timestamp, *got.timestamp, 0)
						}
					}
				})
			})
		}
	}
}

type sqlWireValues struct {
	id        uint64
	text      *string
	number    *float64
	timestamp *time.Time
	hash      *uint64
	bytes     []byte
}

var sqlWireColumnNames = []string{
	"id", "payload_str", "payload_double", "payload_timestamp", "payload_hash", "payload_bytes",
}

var sqlWireColumnTypes = []types.Type{
	types.Uint64, types.NewOptional(types.Text), types.NewOptional(types.Double),
	types.NewOptional(types.Timestamp), types.NewOptional(types.Uint64), types.NewOptional(types.Bytes),
}

func sqlWireResultParts(rows []sqlWireValues) []*Ydb_Query.ExecuteQueryResponsePart {
	parts := make([]*Ydb_Query.ExecuteQueryResponsePart, len(rows))
	for i, row := range rows {
		items := []value.Value{value.Uint64Value(row.id)}
		if row.text == nil {
			for _, t := range []types.Type{types.Text, types.Double, types.Timestamp, types.Uint64, types.Bytes} {
				items = append(items, value.NullValue(t))
			}
		} else {
			items = append(items,
				value.OptionalValue(value.TextValue(*row.text)), value.OptionalValue(value.DoubleValue(*row.number)),
				value.OptionalValue(value.TimestampValue(uint64(row.timestamp.UnixMicro()))),
				value.OptionalValue(value.Uint64Value(*row.hash)), value.OptionalValue(value.BytesValue(row.bytes)),
			)
		}
		rs := &Ydb.ResultSet{Rows: []*Ydb.Value{{Items: make([]*Ydb.Value, len(items))}}}
		for column, item := range items {
			rs.Rows[0].Items[column] = value.ToYDB(item).GetValue()
			if i == 0 {
				rs.Columns = append(rs.Columns, &Ydb.Column{
					Name: sqlWireColumnNames[column], Type: types.TypeToYDB(sqlWireColumnTypes[column]),
				})
			}
		}
		parts[i] = &Ydb_Query.ExecuteQueryResponsePart{Status: Ydb.StatusIds_SUCCESS, ResultSet: rs}
	}

	return parts
}

func sqlWireRef[T any](v T) *T { return &v }
