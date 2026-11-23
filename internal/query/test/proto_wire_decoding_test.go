package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryProtoWireDecoding(t *testing.T) {
	for _, tt := range []struct {
		name    string
		options []ydb.Option
		wire    bool
	}{
		{name: "default", wire: true},
		{name: "enabled", options: []ydb.Option{ydb.WithQueryProtoWireDecoding(true)}, wire: true},
		{name: "disabled", options: []ydb.Option{ydb.WithQueryProtoWireDecoding(false)}},
		{
			name: "last_option_wins",
			options: []ydb.Option{
				ydb.WithQueryProtoWireDecoding(true),
				ydb.WithQueryProtoWireDecoding(false),
			},
		},
		{
			name: "last_option_enables",
			options: []ydb.Option{
				ydb.WithQueryProtoWireDecoding(false),
				ydb.WithQueryProtoWireDecoding(true),
			},
			wire: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, prefetch := range []int{0, 2} {
				t.Run(fmt.Sprintf("prefetch_%d", prefetch), func(t *testing.T) {
					server := mock.Server(t)
					observer := &queryDecoderObserver{}
					opts := append([]ydb.Option{
						ydb.WithAnonymousCredentials(),
						ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(observer.intercept))),
					}, tt.options...)
					db, err := ydb.Open(t.Context(), server.ConnString(), opts...)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })

					t.Run("QueryRow", func(t *testing.T) {
						observer.check(t, tt.wire, func() {
							row, err := db.Query().QueryRow(t.Context(), `SELECT 42`,
								query.WithResponsePartPrefetch(prefetch))
							require.NoError(t, err)
							var got int32
							require.NoError(t, row.Scan(&got))
							require.Equal(t, int32(42), got)
						})
					})

					t.Run("Do", func(t *testing.T) {
						observer.check(t, tt.wire, func() {
							var got int32
							err := db.Query().Do(t.Context(), func(ctx context.Context, session query.Session) error {
								result, err := session.Query(ctx, `SELECT 42`, query.WithResponsePartPrefetch(prefetch))
								if err != nil {
									return err
								}
								defer func() { require.NoError(t, result.Close(ctx)) }()

								rs, err := result.NextResultSet(ctx)
								if err != nil {
									return err
								}
								row, err := rs.NextRow(ctx)
								if err != nil {
									return err
								}

								return row.Scan(&got)
							})
							require.NoError(t, err)
							require.Equal(t, int32(42), got)
						})
					})

					t.Run("database_sql", func(t *testing.T) {
						connector, err := ydb.Connector(db, ydb.WithQueryService(true),
							ydb.WithPrefetchQueryResultParts(prefetch))
						require.NoError(t, err)
						sqlDB := sql.OpenDB(connector)
						defer func() { require.NoError(t, sqlDB.Close()) }()

						t.Run("QueryRow", func(t *testing.T) {
							observer.check(t, tt.wire, func() {
								var got int32
								require.NoError(t, sqlDB.QueryRowContext(t.Context(), `SELECT 42`).Scan(&got))
								require.Equal(t, int32(42), got)
							})
						})
						t.Run("PreparedTransaction", func(t *testing.T) {
							observer.check(t, tt.wire, func() {
								tx, err := sqlDB.BeginTx(t.Context(), nil)
								require.NoError(t, err)
								defer func() { require.NoError(t, tx.Rollback()) }()
								stmt, err := tx.PrepareContext(t.Context(), `SELECT 42`)
								require.NoError(t, err)
								defer func() { require.NoError(t, stmt.Close()) }()
								var got int32
								require.NoError(t, stmt.QueryRowContext(t.Context()).Scan(&got))
								require.Equal(t, int32(42), got)
							})
						})
					})
					t.Run("database_sql_table_service", func(t *testing.T) {
						queryCalls, tableCalls := server.ExecuteQueryCalls(), server.ExecuteDataQueryCalls()
						connector, err := ydb.Connector(db, ydb.WithQueryService(false))
						require.NoError(t, err)
						sqlDB := sql.OpenDB(connector)
						defer func() { require.NoError(t, sqlDB.Close()) }()
						var got int32
						require.NoError(t, sqlDB.QueryRowContext(t.Context(), `SELECT 42`).Scan(&got))
						require.Equal(t, int32(42), got)
						require.Equal(t, queryCalls, server.ExecuteQueryCalls())
						require.Greater(t, server.ExecuteDataQueryCalls(), tableCalls)
					})
				})
			}
		})
	}
}

type queryDecoderObserver struct {
	standard atomic.Int64
	wire     atomic.Int64
}

func (o *queryDecoderObserver) check(t *testing.T, wire bool, query func()) {
	t.Helper()

	standardBefore, wireBefore := o.standard.Load(), o.wire.Load()
	query()
	if wire {
		require.Equal(t, standardBefore, o.standard.Load(), "standard protobuf decoder was selected")
		require.Greater(t, o.wire.Load(), wireBefore, "protobuf wire decoder was not selected")
	} else {
		require.Greater(t, o.standard.Load(), standardBefore, "standard protobuf decoder was not selected")
		require.Equal(t, wireBefore, o.wire.Load(), "protobuf wire decoder was selected")
	}
}

func (o *queryDecoderObserver) intercept(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil || method != Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName {
		return stream, err
	}

	return queryDecoderStream{ClientStream: stream, observer: o}, nil
}

type queryDecoderStream struct {
	grpc.ClientStream

	observer *queryDecoderObserver
}

func (s queryDecoderStream) RecvMsg(message any) error {
	if err := s.ClientStream.RecvMsg(message); err != nil {
		return err
	}
	part := message.(*Ydb_Query.ExecuteQueryResponsePart)
	if part.GetResultSet() == nil {
		return nil
	}
	// SELECT 42 always returns a row. Wire decoding retains it outside the
	// protobuf response while preserving that response for interceptors.
	if len(part.GetResultSet().GetRows()) == 0 {
		s.observer.wire.Add(1)
	} else {
		s.observer.standard.Add(1)
	}

	return nil
}
