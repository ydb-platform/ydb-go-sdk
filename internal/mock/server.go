// Package mock provides an in-process YDB gRPC stack (Discovery + Table + Query) with fixed
// "SELECT 42" responses for benchmarks and tests. It does not require a real YDB endpoint.
package mock

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
)

// server is a local gRPC mock (Discovery + Table + Query).
type server struct {
	listener   net.Listener
	grpcServer *grpc.Server

	querySessionID atomic.Uint64
	tableSessionID atomic.Uint64

	// executeQueryCalls counts QueryService.ExecuteQuery invocations.
	// executeDataQueryCalls counts TableService.ExecuteDataQuery invocations.
	// Both are used by integration tests to assert which gRPC method was
	// dispatched to (e.g. when toggling WithExecuteDataQueryOverQueryClient).
	executeQueryCalls     atomic.Uint64
	executeDataQueryCalls atomic.Uint64
}

func (m *server) nextQuerySession() string {
	return fmt.Sprintf("q-%d", m.querySessionID.Add(1))
}

func (m *server) nextTableSession() string {
	return fmt.Sprintf("t-%d", m.tableSessionID.Add(1))
}

// ExecuteQueryCalls returns the number of times QueryService.ExecuteQuery
// has been invoked on this mock since it was started.
func (m *server) ExecuteQueryCalls() uint64 {
	return m.executeQueryCalls.Load()
}

// ExecuteDataQueryCalls returns the number of times TableService.ExecuteDataQuery
// has been invoked on this mock since it was started.
func (m *server) ExecuteDataQueryCalls() uint64 {
	return m.executeDataQueryCalls.Load()
}

// ConnString returns a grpc:// DSN for ydb.Open pointing at this mock.
func (m *server) ConnString() string {
	return fmt.Sprintf("grpc://%s/local", m.listener.Addr().String())
}

// Close stops the gRPC server and closes the listener.
func (m *server) Close() {
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

type discoverySrv struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	host string
	port uint32
}

func (m *discoverySrv) ListEndpoints(
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

func (m *discoverySrv) WhoAmI(
	_ context.Context,
	_ *Ydb_Discovery.WhoAmIRequest,
) (*Ydb_Discovery.WhoAmIResponse, error) {
	return &Ydb_Discovery.WhoAmIResponse{
		Operation: operationOK(&emptypb.Empty{}),
	}, nil
}

type tableSrv struct {
	Ydb_Table_V1.UnimplementedTableServiceServer

	mock *server
}

func (m *tableSrv) CreateSession(
	_ context.Context,
	_ *Ydb_Table.CreateSessionRequest,
) (*Ydb_Table.CreateSessionResponse, error) {
	return &Ydb_Table.CreateSessionResponse{
		Operation: operationOK(&Ydb_Table.CreateSessionResult{
			SessionId: m.mock.nextTableSession(),
		}),
	}, nil
}

func (m *tableSrv) DeleteSession(
	_ context.Context,
	_ *Ydb_Table.DeleteSessionRequest,
) (*Ydb_Table.DeleteSessionResponse, error) {
	return &Ydb_Table.DeleteSessionResponse{}, nil
}

func (m *tableSrv) KeepAlive(
	_ context.Context,
	_ *Ydb_Table.KeepAliveRequest,
) (*Ydb_Table.KeepAliveResponse, error) {
	return &Ydb_Table.KeepAliveResponse{
		Operation: operationOK(&Ydb_Table.KeepAliveResult{
			SessionStatus: Ydb_Table.KeepAliveResult_SESSION_STATUS_READY,
		}),
	}, nil
}

func (m *tableSrv) ExecuteDataQuery(
	_ context.Context,
	req *Ydb_Table.ExecuteDataQueryRequest,
) (*Ydb_Table.ExecuteDataQueryResponse, error) {
	m.mock.executeDataQueryCalls.Add(1)

	return &Ydb_Table.ExecuteDataQueryResponse{
		Operation: operationOK(&Ydb_Table.ExecuteQueryResult{
			ResultSets: resultSetsForQuery(req.GetQuery().GetYqlText()),
		}),
	}, nil
}

type querySrv struct {
	Ydb_Query_V1.UnimplementedQueryServiceServer

	mock *server
}

func (m *querySrv) CreateSession(
	_ context.Context,
	_ *Ydb_Query.CreateSessionRequest,
) (*Ydb_Query.CreateSessionResponse, error) {
	return &Ydb_Query.CreateSessionResponse{
		Status:    Ydb.StatusIds_SUCCESS,
		SessionId: m.mock.nextQuerySession(),
		NodeId:    1,
	}, nil
}

func (m *querySrv) DeleteSession(
	_ context.Context,
	_ *Ydb_Query.DeleteSessionRequest,
) (*Ydb_Query.DeleteSessionResponse, error) {
	return &Ydb_Query.DeleteSessionResponse{
		Status: Ydb.StatusIds_SUCCESS,
	}, nil
}

func (m *querySrv) AttachSession(
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

func (m *querySrv) ExecuteQuery(
	req *Ydb_Query.ExecuteQueryRequest,
	stream Ydb_Query_V1.QueryService_ExecuteQueryServer,
) error {
	m.mock.executeQueryCalls.Add(1)

	resultSets := resultSetsForQuery(req.GetQueryContent().GetText())

	for i, rs := range resultSets {
		err := stream.Send(&Ydb_Query.ExecuteQueryResponsePart{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: int64(i),
			ResultSet:      rs,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// projectionRegex matches a single SELECT projection of the form
// <literal> [AS <name>], where <literal> is an integer, a Utf8 string
// ("text"u) or a String/bytes literal ("text"). Multiple projections per
// statement are supported by FindAllStringSubmatch.
var projectionRegex = regexp.MustCompile(`(?i)("[^"]*"u|"[^"]*"|-?\d+)(?:\s+AS\s+(\w+))?`)

// resultSetsForQuery converts a (possibly multi-statement) SQL string into one
// result set per non-empty statement. Each statement may contain multiple
// projections separated by ',' producing one column per projection.
// Unknown shapes fall back to select42ResultSet so existing "SELECT 42"
// tests keep working.
func resultSetsForQuery(query string) []*Ydb.ResultSet {
	statements := splitStatements(query)
	if len(statements) == 0 {
		return []*Ydb.ResultSet{select42ResultSet()}
	}

	resultSets := make([]*Ydb.ResultSet, 0, len(statements))
	for _, stmt := range statements {
		resultSets = append(resultSets, resultSetForStatement(stmt))
	}

	return resultSets
}

// splitStatements splits sql by ';', dropping empty / whitespace-only fragments.
// It is intentionally simplistic: the mock does not need a real SQL parser, only
// enough to model multi-statements such as
// `SELECT 42 AS id; SELECT "hello"u AS hello, "world" AS world`.
func splitStatements(query string) []string {
	parts := strings.Split(query, ";")
	statements := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			statements = append(statements, p)
		}
	}

	return statements
}

// resultSetForStatement builds a result set with one column per projection
// found in stmt. Recognized literals: integer, Utf8 string ("text"u) and
// bytes string ("text"). If no projection matches, select42ResultSet is used
// as a fallback for backward compatibility with simple "SELECT 42" tests.
func resultSetForStatement(stmt string) *Ydb.ResultSet {
	matches := projectionRegex.FindAllStringSubmatch(stmt, -1)
	if len(matches) == 0 {
		return select42ResultSet()
	}

	columns := make([]*Ydb.Column, 0, len(matches))
	items := make([]*Ydb.Value, 0, len(matches))
	for _, m := range matches {
		col, val := projectionColumn(m[1], m[2])
		columns = append(columns, col)
		items = append(items, val)
	}

	return &Ydb.ResultSet{
		Columns: columns,
		Rows: []*Ydb.Value{
			{Items: items},
		},
	}
}

// projectionColumn maps a parsed (literal, name) pair to an Ydb column
// descriptor and the matching row item.
func projectionColumn(literal, name string) (*Ydb.Column, *Ydb.Value) {
	switch {
	case strings.HasPrefix(literal, `"`) && strings.HasSuffix(strings.ToLower(literal), `"u`):
		return utf8Column(name, literal[1:len(literal)-2])
	case strings.HasPrefix(literal, `"`) && strings.HasSuffix(literal, `"`):
		return bytesColumn(name, []byte(literal[1:len(literal)-1]))
	default:
		n, _ := strconv.ParseInt(literal, 10, 32)

		return int32Column(name, int32(n))
	}
}

func int32Column(name string, value int32) (*Ydb.Column, *Ydb.Value) {
	return &Ydb.Column{
			Name: name,
			Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32}},
		}, &Ydb.Value{
			Value: &Ydb.Value_Int32Value{Int32Value: value},
		}
}

func utf8Column(name, value string) (*Ydb.Column, *Ydb.Value) {
	return &Ydb.Column{
			Name: name,
			Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UTF8}},
		}, &Ydb.Value{
			Value: &Ydb.Value_TextValue{TextValue: value},
		}
}

func bytesColumn(name string, value []byte) (*Ydb.Column, *Ydb.Value) {
	return &Ydb.Column{
			Name: name,
			Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_STRING}},
		}, &Ydb.Value{
			Value: &Ydb.Value_BytesValue{BytesValue: value},
		}
}

func select42ResultSet() *Ydb.ResultSet {
	col, val := int32Column("", 42)

	return &Ydb.ResultSet{
		Columns: []*Ydb.Column{col},
		Rows:    []*Ydb.Value{{Items: []*Ydb.Value{val}}},
	}
}

// Server binds a random TCP port, registers Discovery + Table + Query mocks, and serves until Close or tb cleanup.
func Server(tb testing.TB) *server {
	tb.Helper()

	lc := net.ListenConfig{}

	lis, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(tb, err)

	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(tb, err)

	m := &server{
		listener:   lis,
		grpcServer: grpc.NewServer(),
	}

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(m.grpcServer, &discoverySrv{
		host: host,
		port: uint32(port),
	})
	Ydb_Table_V1.RegisterTableServiceServer(m.grpcServer, &tableSrv{mock: m})
	Ydb_Query_V1.RegisterQueryServiceServer(m.grpcServer, &querySrv{mock: m})

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
