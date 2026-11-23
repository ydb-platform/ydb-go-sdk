package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

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
		{name: "default"},
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

func TestQueryProtoWireDecodingRespectsDriverCodec(t *testing.T) {
	codec := rewritingQueryResultCodec{CodecV2: encoding.GetCodecV2("proto"), value: 9001}
	encoding.RegisterCodecV2(registeredQueryResultCodec{codec})
	interceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(ctx, desc, cc, method, append(opts, grpc.ForceCodecV2(codec))...)
	}
	for _, tt := range []struct {
		name string
		dial grpc.DialOption
	}{
		{name: "ForceCodecV2", dial: grpc.WithDefaultCallOptions(grpc.ForceCodecV2(codec))},
		{name: "ForceCodec", dial: grpc.WithDefaultCallOptions(grpc.ForceCodec(queryResultCodecV1{codec}))},
		{name: "legacy", dial: grpc.WithCodec(queryResultLegacyCodec{queryResultCodecV1{codec}})}, //nolint:staticcheck,nolintlint
		{name: "registered_subtype", dial: grpc.WithDefaultCallOptions(grpc.CallContentSubtype(codecName))},
		{name: "stream_interceptor", dial: grpc.WithStreamInterceptor(interceptor)},
		{name: "chained_stream_interceptor", dial: grpc.WithChainStreamInterceptor(interceptor)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, wire := range []bool{false, true} {
				t.Run(fmt.Sprintf("wire_%t", wire), func(t *testing.T) {
					server := mock.Server(t)
					db, err := ydb.Open(t.Context(), server.ConnString(),
						ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(wire),
						ydb.With(config.WithGrpcOptions(tt.dial)),
					)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })
					for _, prefetch := range []int{0, 2} {
						t.Run(fmt.Sprintf("prefetch_%d", prefetch), func(t *testing.T) {
							t.Run("native", func(t *testing.T) {
								row, err := db.Query().QueryRow(t.Context(), "SELECT 42", query.WithResponsePartPrefetch(prefetch))
								require.NoError(t, err)
								var got int32
								require.NoError(t, row.Scan(&got))
								require.Equal(t, int32(9001), got, "the driver codec must determine the decoded result")
							})
							t.Run("database_sql", func(t *testing.T) {
								connector, err := ydb.Connector(db, ydb.WithQueryService(true), ydb.WithPrefetchQueryResultParts(prefetch))
								require.NoError(t, err)
								sqlDB := sql.OpenDB(connector)
								t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
								var got int32
								require.NoError(t, sqlDB.QueryRowContext(t.Context(), "SELECT 42").Scan(&got))
								require.Equal(t, int32(9001), got, "the driver codec must determine the decoded result")
							})
						})
					}
				})
			}
		})
	}
}

func TestQueryProtoWireDecodingRespectsQueryCodecOverride(t *testing.T) {
	protobuf := encoding.GetCodecV2("proto")
	driverCodec := rewritingQueryResultCodec{CodecV2: protobuf, value: 9001}
	queryCodec := rewritingQueryResultCodec{CodecV2: protobuf, value: 9002}
	for _, tt := range []struct {
		name     string
		defaults grpc.CallOption
		query    grpc.CallOption
	}{
		{
			name: "V1_over_V2", defaults: grpc.ForceCodecV2(driverCodec),
			query: grpc.ForceCodec(queryResultCodecV1{queryCodec}),
		},
		{
			name: "V2_over_V1", defaults: grpc.ForceCodec(queryResultCodecV1{driverCodec}),
			query: grpc.ForceCodecV2(queryCodec),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := mock.Server(t)
			db, err := ydb.Open(t.Context(), server.ConnString(),
				ydb.WithAnonymousCredentials(), ydb.WithQueryProtoWireDecoding(true),
				ydb.With(config.WithGrpcOptions(grpc.WithDefaultCallOptions(tt.defaults))),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close(t.Context())) })
			for _, prefetch := range []int{0, 2} {
				t.Run(fmt.Sprintf("prefetch_%d", prefetch), func(t *testing.T) {
					row, err := db.Query().QueryRow(t.Context(), "SELECT 42",
						query.WithResponsePartPrefetch(prefetch), query.WithCallOptions(tt.query))
					require.NoError(t, err)
					var got int32
					require.NoError(t, row.Scan(&got))
					require.Equal(t, int32(9002), got, "a query codec must take precedence over the driver codec")
				})
			}
		})
	}
}

const codecName = "query-driver-codec-test"

type rewritingQueryResultCodec struct {
	encoding.CodecV2
	value int32
}

func (c rewritingQueryResultCodec) Unmarshal(data mem.BufferSlice, dst any) error {
	if err := c.CodecV2.Unmarshal(data, dst); err != nil {
		return err
	}
	if part, ok := dst.(*Ydb_Query.ExecuteQueryResponsePart); ok {
		for _, row := range part.GetResultSet().GetRows() {
			for _, value := range row.GetItems() {
				if n, ok := value.GetValue().(*Ydb.Value_Int32Value); ok {
					n.Int32Value = c.value
				}
			}
		}
	}

	return nil
}

type registeredQueryResultCodec struct{ rewritingQueryResultCodec }

func (registeredQueryResultCodec) Name() string { return codecName }

type queryResultCodecV1 struct{ rewritingQueryResultCodec }

func (c queryResultCodecV1) Marshal(src any) ([]byte, error) {
	data, err := c.CodecV2.Marshal(src)
	if err != nil {
		return nil, err
	}
	defer data.Free()
	frame := make([]byte, data.Len())
	data.CopyTo(frame)

	return frame, nil
}

func (c queryResultCodecV1) Unmarshal(data []byte, dst any) error {
	return c.rewritingQueryResultCodec.Unmarshal(mem.BufferSlice{mem.SliceBuffer(data)}, dst)
}

type queryResultLegacyCodec struct{ queryResultCodecV1 }

func (c queryResultLegacyCodec) String() string { return c.Name() }

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
