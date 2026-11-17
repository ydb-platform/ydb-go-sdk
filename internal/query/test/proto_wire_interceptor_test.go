package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryProtoWireInterceptorReceivesProtobuf(t *testing.T) {
	server := mock.Server(t)
	var received atomic.Int32
	db, err := ydb.Open(t.Context(), server.ConnString(),
		ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(true),
		ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(queryResponseInterceptor(
			func(message any) error {
				if _, ok := message.(*Ydb_Query.ExecuteQueryResponsePart); !ok {
					return fmt.Errorf("query interceptor: expected protobuf response, got %T", message)
				}
				received.Add(1)

				return nil
			},
		)))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })

	row, err := db.Query().QueryRow(t.Context(), "SELECT 42")
	require.NoError(t, err)
	var got int32
	require.NoError(t, row.Scan(&got))
	require.Equal(t, int32(42), got)
	require.Equal(t, int32(1), received.Load(), "the interceptor must observe the query response")
}

func TestQueryProtoWireInterceptorRejectsTruncation(t *testing.T) {
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			for _, truncatedPart := range []int{0, 1} {
				t.Run(fmt.Sprintf("wire=%v/prefetch=%d/part=%d", wire, prefetch, truncatedPart), func(t *testing.T) {
					parts := interceptedQueryParts()
					if truncatedPart == 1 {
						parts[1].ResultSet = &Ydb.ResultSet{}
					}
					parts[truncatedPart].ResultSet.Truncated = true
					var rejected atomic.Int32
					db := openInterceptedQuery(t, wire, parts, func(message any) error {
						if part, ok := message.(*Ydb_Query.ExecuteQueryResponsePart); ok && part.GetResultSet().GetTruncated() {
							rejected.Add(1)

							return fmt.Errorf("query result is truncated")
						}

						return nil
					})
					result, err := db.Query().Query(t.Context(), "SELECT 42", query.WithResponsePartPrefetch(prefetch))
					require.ErrorContains(t, err, "query result is truncated")
					require.Nil(t, result, "rows read before a rejected response must not escape a materialized query")
					require.Equal(t, int32(1), rejected.Load())
				})
			}
		}
	}
}

func TestQueryProtoWireInterceptorObservesAndUpdatesStatistics(t *testing.T) {
	for _, wire := range []bool{false, true} {
		for _, prefetch := range []int{0, 2} {
			for _, sqlQuery := range []bool{false, true} {
				t.Run(fmt.Sprintf("wire=%v/prefetch=%d/sql=%v", wire, prefetch, sqlQuery), func(t *testing.T) {
					var observed atomic.Int64
					db := openInterceptedQuery(t, wire, interceptedQueryParts(), func(message any) error {
						if part, ok := message.(*Ydb_Query.ExecuteQueryResponsePart); ok && part.GetExecStats() != nil {
							observed.Store(int64(part.GetExecStats().GetTotalDurationUs()))
							part.ExecStats = &Ydb_TableStats.QueryStats{TotalDurationUs: 456}
						}

						return nil
					})
					var got int32
					if sqlQuery {
						connector, err := ydb.Connector(db, ydb.WithQueryService(true),
							ydb.WithPrefetchQueryResultParts(prefetch))
						require.NoError(t, err)
						sqlDB := sql.OpenDB(connector)
						t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
						require.NoError(t, sqlDB.QueryRowContext(t.Context(), "SELECT 42").Scan(&got))
					} else {
						var total time.Duration
						row, err := db.Query().QueryRow(t.Context(), "SELECT 42", query.WithResponsePartPrefetch(prefetch),
							query.WithStatsMode(query.StatsModeBasic, func(stats query.Stats) { total = stats.TotalDuration() }))
						require.NoError(t, err)
						require.NoError(t, row.Scan(&got))
						require.Equal(t, 456*time.Microsecond, total, "interceptor metadata changes must reach the caller")
					}
					require.Equal(t, int32(42), got)
					require.Equal(t, int64(123), observed.Load(), "the interceptor must observe trailing query statistics")
				})
			}
		}
	}
}

func openInterceptedQuery(t *testing.T, wire bool, parts []*Ydb_Query.ExecuteQueryResponsePart,
	onPart func(any) error,
) *ydb.Driver {
	t.Helper()
	server := mock.Server(t, mock.WithExecuteQueryParts(parts...))
	db, err := ydb.Open(t.Context(), server.ConnString(),
		ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(wire),
		ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(queryResponseInterceptor(onPart)))),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })

	return db
}

func interceptedQueryParts() []*Ydb_Query.ExecuteQueryResponsePart {
	return []*Ydb_Query.ExecuteQueryResponsePart{
		{
			Status: Ydb.StatusIds_SUCCESS,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{{Name: "value", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}}}},
				Rows:    []*Ydb.Value{{Items: []*Ydb.Value{{Value: &Ydb.Value_Int32Value{Int32Value: 42}}}}},
			},
		},
		{Status: Ydb.StatusIds_SUCCESS, ExecStats: &Ydb_TableStats.QueryStats{TotalDurationUs: 123}},
	}
}

func queryResponseInterceptor(onPart func(any) error) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || method != Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName {
			return stream, err
		}

		return queryResponseStream{ClientStream: stream, onPart: onPart}, nil
	}
}

type queryResponseStream struct {
	grpc.ClientStream

	onPart func(any) error
}

func (s queryResponseStream) RecvMsg(message any) error {
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return err
	}

	return s.onPart(message)
}
