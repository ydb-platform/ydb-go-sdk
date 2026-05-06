package bench

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

// cpu: Apple M3 Pro
// go test -bench=. -benchtime=10s .
//
// BenchmarkDatabaseSQLMock/QueryService-12  1093219  11897 ns/op   22996 B/op    387 allocs/op
// BenchmarkDatabaseSQLMock/TableService-12  1301697  11099 ns/op   18580 B/op    306 allocs/op
// Diff (query/table*100-100)                   -7%          23%          23%              26%

// grpcMockYDB is a local in-process gRPC stack (Discovery + Table + Query) with fixed
// "SELECT 42" style responses. It does not require a real YDB endpoint.
type grpcMockYDB struct {
	listener   net.Listener
	grpcServer *grpc.Server

	querySessionID atomic.Uint64
	tableSessionID atomic.Uint64
}

func (m *grpcMockYDB) nextQuerySession() string {
	return fmt.Sprintf("q-%d", m.querySessionID.Add(1))
}

func (m *grpcMockYDB) nextTableSession() string {
	return fmt.Sprintf("t-%d", m.tableSessionID.Add(1))
}

func (m *grpcMockYDB) connString() string {
	return fmt.Sprintf("grpc://%s/local", m.listener.Addr().String())
}

func (m *grpcMockYDB) Close() {
	m.grpcServer.Stop()

	_ = m.listener.Close()
}

func mustMarshalAny(msg proto.Message) *anypb.Any {
	a := &anypb.Any{}
	if err := a.MarshalFrom(msg); err != nil {
		panic(err)
	}

	return a
}

func operationOK(msg proto.Message) *Ydb_Operations.Operation {
	return &Ydb_Operations.Operation{
		Id:     "mock-op",
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Result: mustMarshalAny(msg),
	}
}

type mockDiscovery struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	host string
	port uint32
}

func (m *mockDiscovery) ListEndpoints(
	_ context.Context,
	_ *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	res := &Ydb_Discovery.ListEndpointsResult{
		Endpoints: []*Ydb_Discovery.EndpointInfo{
			{
				Address:    m.host,
				Port:       m.port,
				LoadFactor: 0,
				Ssl:        false,
				Service:    nil,
				Location:   "",
				NodeId:     1,
				IpV4:       []string{"127.0.0.1"},
			},
		},
		SelfLocation: "",
	}

	return &Ydb_Discovery.ListEndpointsResponse{
		Operation: operationOK(res),
	}, nil
}

func (m *mockDiscovery) WhoAmI(
	_ context.Context,
	_ *Ydb_Discovery.WhoAmIRequest,
) (*Ydb_Discovery.WhoAmIResponse, error) {
	return &Ydb_Discovery.WhoAmIResponse{
		Operation: operationOK(&emptypb.Empty{}),
	}, nil
}

type mockTable struct {
	Ydb_Table_V1.UnimplementedTableServiceServer

	mock *grpcMockYDB
}

func (m *mockTable) CreateSession(
	_ context.Context,
	_ *Ydb_Table.CreateSessionRequest,
) (*Ydb_Table.CreateSessionResponse, error) {
	return &Ydb_Table.CreateSessionResponse{
		Operation: operationOK(&Ydb_Table.CreateSessionResult{
			SessionId: m.mock.nextTableSession(),
		}),
	}, nil
}

func (m *mockTable) DeleteSession(
	_ context.Context,
	_ *Ydb_Table.DeleteSessionRequest,
) (*Ydb_Table.DeleteSessionResponse, error) {
	return &Ydb_Table.DeleteSessionResponse{}, nil
}

func (m *mockTable) KeepAlive(
	_ context.Context,
	_ *Ydb_Table.KeepAliveRequest,
) (*Ydb_Table.KeepAliveResponse, error) {
	return &Ydb_Table.KeepAliveResponse{
		Operation: operationOK(&Ydb_Table.KeepAliveResult{
			SessionStatus: Ydb_Table.KeepAliveResult_SESSION_STATUS_READY,
		}),
	}, nil
}

func (m *mockTable) ExecuteDataQuery(
	_ context.Context,
	_ *Ydb_Table.ExecuteDataQueryRequest,
) (*Ydb_Table.ExecuteDataQueryResponse, error) {
	rs := select42ResultSet()

	return &Ydb_Table.ExecuteDataQueryResponse{
		Operation: operationOK(&Ydb_Table.ExecuteQueryResult{
			ResultSets: []*Ydb.ResultSet{rs},
		}),
	}, nil
}

type mockQuery struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	mock *grpcMockYDB
}

func (m *mockQuery) CreateSession(
	_ context.Context,
	_ *Ydb_Query.CreateSessionRequest,
) (*Ydb_Query.CreateSessionResponse, error) {
	return &Ydb_Query.CreateSessionResponse{
		Status:    Ydb.StatusIds_SUCCESS,
		SessionId: m.mock.nextQuerySession(),
		NodeId:    1,
	}, nil
}

func (m *mockQuery) DeleteSession(
	_ context.Context,
	_ *Ydb_Query.DeleteSessionRequest,
) (*Ydb_Query.DeleteSessionResponse, error) {
	return &Ydb_Query.DeleteSessionResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}

func (m *mockQuery) AttachSession(
	_ *Ydb_Query.AttachSessionRequest,
	stream Ydb_Query_V1.QueryService_AttachSessionServer,
) error {
	err := stream.Send(&Ydb_Query.SessionState{
		Status: Ydb.StatusIds_SUCCESS,
	})
	if err != nil {
		return err
	}

	<-stream.Context().Done()

	return nil
}

func (m *mockQuery) ExecuteQuery(
	_ *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	rs := select42ResultSet()

	return stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet:      rs,
	})
}

func select42ResultSet() *Ydb.ResultSet {
	return &Ydb.ResultSet{
		Columns: []*Ydb.Column{
			{
				Name: "",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_INT32,
					},
				},
			},
		},
		Rows: []*Ydb.Value{
			{
				Items: []*Ydb.Value{
					{
						Value: &Ydb.Value_Int32Value{
							Int32Value: 42,
						},
					},
				},
			},
		},
	}
}

func startMockYDBServer(tb testing.TB) *grpcMockYDB {
	tb.Helper()

	lc := net.ListenConfig{}

	lis, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(tb, err)

	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(tb, err)

	m := &grpcMockYDB{
		listener:   lis,
		grpcServer: grpc.NewServer(),
	}

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(m.grpcServer, &mockDiscovery{
		host: host,
		port: uint32(port),
	})
	Ydb_Table_V1.RegisterTableServiceServer(m.grpcServer, &mockTable{mock: m})
	Ydb_Query_V1.RegisterQueryServiceServer(m.grpcServer, &mockQuery{mock: m})

	go func() {
		_ = m.grpcServer.Serve(lis)
	}()

	tb.Cleanup(m.Close)

	for range 100 {
		conn, err := net.Dial("tcp", lis.Addr().String()) //nolint:noctx
		if err == nil {
			conn.Close()

			break
		}
		time.Sleep(time.Millisecond)
	}

	return m
}

const benchmarkDatabaseSQLMockSessionPoolSize = 600

func BenchmarkDatabaseSQLMock(b *testing.B) {
	ctx := b.Context()
	parallelism := 100

	mockSrv := startMockYDBServer(b)

	nativeDriver, err := ydb.Open(ctx, mockSrv.connString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithSessionPoolSizeLimit(benchmarkDatabaseSQLMockSessionPoolSize),
	)
	require.NoError(b, err)

	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	for _, engine := range []struct {
		name            string
		useQueryService bool
	}{
		{
			name:            "QueryService",
			useQueryService: true,
		},
		{
			name:            "TableService",
			useQueryService: false,
		},
	} {
		b.Run(engine.name, func(b *testing.B) {
			connector, err := ydb.Connector(nativeDriver,
				ydb.WithQueryService(engine.useQueryService),
			)
			require.NoError(b, err)

			defer func() {
				_ = connector.Close()
			}()

			db := sql.OpenDB(connector)
			defer func() {
				_ = db.Close()
			}()

			db.SetMaxOpenConns(benchmarkDatabaseSQLMockSessionPoolSize)

			warmUpMock(ctx, b, db)

			b.SetParallelism(parallelism)
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var (
					v    int
					rows *sql.Rows
					err  error
				)

				for pb.Next() {
					func() {
						rows, err = db.QueryContext(ctx, `SELECT 42`)
						if !assert.NoError(b, err) {
							return
						}
						defer func() {
							assert.NoError(b, rows.Close())
						}()

						for rows.Next() {
							if !assert.NoError(b, rows.Scan(&v)) {
								return
							}
							if !assert.Equal(b, 42, v) {
								return
							}
							v = 0
						}

						assert.NoError(b, rows.Err())
					}()
				}
			})
		})
	}
}

func warmUpMock(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()

	wg := sync.WaitGroup{}
	wg.Add(benchmarkDatabaseSQLMockSessionPoolSize)

	for range benchmarkDatabaseSQLMockSessionPoolSize {
		go func() {
			defer wg.Done()
			rows, err := db.QueryContext(ctx, `SELECT 42`)
			if !assert.NoError(t, err) {
				return
			}
			defer func() {
				assert.NoError(t, rows.Close())
			}()

			var v int

			for rows.Next() {
				if !assert.NoError(t, rows.Scan(&v)) {
					return
				}
			}

			assert.NoError(t, rows.Err())
		}()
	}

	wg.Wait()
}
