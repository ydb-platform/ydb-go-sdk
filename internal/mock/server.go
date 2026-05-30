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

// ServerOption configures mock server behavior.
type ServerOption func(*server)

// WithClusterNodes makes discovery report multiple endpoints that share the
// mock listener address but have distinct node IDs.
func WithClusterNodes(nodeIDs ...uint32) ServerOption {
	return func(m *server) {
		m.clusterNodeIDs = append([]uint32(nil), nodeIDs...)
	}
}

// server is a local gRPC mock (Discovery + Table + Query).
type server struct {
	listener   net.Listener
	grpcServer *grpc.Server

	querySessionID atomic.Uint64
	tableSessionID atomic.Uint64

	clusterNodeIDs []uint32

	// nodeShutdownSignal unblocks AttachSession handlers waiting to send a
	// NodeShutdown hint (see TriggerNodeShutdown).
	nodeShutdownSignal chan struct{}

	// executeQueryCalls counts QueryService.ExecuteQuery invocations.
	// executeDataQueryCalls counts TableService.ExecuteDataQuery invocations.
	// Both are used by integration tests to assert which gRPC method was
	// dispatched to (e.g. when toggling WithExecuteDataQueryOverQueryClient).
	executeQueryCalls     atomic.Uint64
	executeDataQueryCalls atomic.Uint64
}

// TriggerNodeShutdown makes one AttachSession handler send a
// NodeShutdown hint after its initial SessionState.
func (m *server) TriggerNodeShutdown() {
	select {
	case m.nodeShutdownSignal <- struct{}{}:
	default:
	}
}

func (m *server) nextQuerySession() (string, uint32) {
	id := m.querySessionID.Add(1)

	return fmt.Sprintf("q-%d", id), m.querySessionNodeID(id)
}

func (m *server) nextTableSession() string {
	return fmt.Sprintf("t-%d", m.tableSessionID.Add(1))
}

func (m *server) querySessionNodeID(sessionID uint64) uint32 {
	if len(m.clusterNodeIDs) == 0 {
		return 1
	}

	return m.clusterNodeIDs[(sessionID-1)%uint64(len(m.clusterNodeIDs))]
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

	endpoints []*Ydb_Discovery.EndpointInfo
}

func (m *discoverySrv) ListEndpoints(
	_ context.Context,
	_ *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	res := &Ydb_Discovery.ListEndpointsResult{
		Endpoints:    m.endpoints,
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
	sessionID, nodeID := m.mock.nextQuerySession()

	return &Ydb_Query.CreateSessionResponse{
		Status:    Ydb.StatusIds_SUCCESS,
		SessionId: sessionID,
		NodeId:    int64(nodeID),
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

	select {
	case <-m.mock.nodeShutdownSignal:
		return stream.Send(&Ydb_Query.SessionState{
			Status: Ydb.StatusIds_SUCCESS,
			SessionHint: &Ydb_Query.SessionState_NodeShutdown{
				NodeShutdown: &Ydb_Query.NodeShutdownHint{},
			},
		})
	case <-stream.Context().Done():
		return nil
	}
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

func discoveryEndpoints(host string, port uint32, nodeIDs []uint32) []*Ydb_Discovery.EndpointInfo {
	endpoints := make([]*Ydb_Discovery.EndpointInfo, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		endpoints[i] = &Ydb_Discovery.EndpointInfo{
			Address:    host,
			Port:       port,
			LoadFactor: 0,
			Ssl:        false,
			Service:    nil,
			Location:   "",
			NodeId:     nodeID,
			IpV4:       []string{"127.0.0.1"},
		}
	}

	return endpoints
}

func waitListenerReady(tb testing.TB, addr string) {
	tb.Helper()

	for range 100 {
		c, err := net.Dial("tcp", addr) //nolint:noctx
		if err == nil {
			_ = c.Close()

			return
		}
		time.Sleep(time.Millisecond)
	}

	tb.Fatalf("mock gRPC listener did not become ready: %s", addr)
}

// Server binds a random TCP port, registers Discovery + Table + Query mocks, and serves until Close or tb cleanup.
func Server(tb testing.TB, opts ...ServerOption) *server {
	tb.Helper()

	lc := net.ListenConfig{}

	lis, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(tb, err)

	port, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(tb, err)

	m := &server{
		listener:           lis,
		grpcServer:         grpc.NewServer(),
		nodeShutdownSignal: make(chan struct{}, 1),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}

	nodeIDs := m.clusterNodeIDs
	if len(nodeIDs) == 0 {
		nodeIDs = []uint32{1}
	}

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(m.grpcServer, &discoverySrv{
		endpoints: discoveryEndpoints(host, uint32(port), nodeIDs),
	})
	Ydb_Table_V1.RegisterTableServiceServer(m.grpcServer, &tableSrv{mock: m})
	Ydb_Query_V1.RegisterQueryServiceServer(m.grpcServer, &querySrv{mock: m})

	go func() {
		_ = m.grpcServer.Serve(lis)
	}()

	tb.Cleanup(m.Close)

	waitListenerReady(tb, lis.Addr().String())

	return m
}
