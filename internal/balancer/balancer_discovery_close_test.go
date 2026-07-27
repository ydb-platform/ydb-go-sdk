package balancer

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Discovery_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Discovery"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type dynamicDiscoveryServer struct {
	listener   net.Listener
	grpcServer *grpc.Server

	mu      sync.RWMutex
	nodeIDs []uint32
	host    string
	port    uint32

	activeConns atomic.Int64
}

type dynamicDiscoveryService struct {
	Ydb_Discovery_V1.UnimplementedDiscoveryServiceServer

	srv *dynamicDiscoveryServer
}

func (s *dynamicDiscoveryService) ListEndpoints(
	_ context.Context,
	_ *Ydb_Discovery.ListEndpointsRequest,
) (*Ydb_Discovery.ListEndpointsResponse, error) {
	endpoints := s.srv.currentEndpoints()

	return &Ydb_Discovery.ListEndpointsResponse{
		Operation: discoveryOperationOK(&Ydb_Discovery.ListEndpointsResult{
			Endpoints: endpoints,
		}),
	}, nil
}

func (s *dynamicDiscoveryService) WhoAmI(
	_ context.Context,
	_ *Ydb_Discovery.WhoAmIRequest,
) (*Ydb_Discovery.WhoAmIResponse, error) {
	return &Ydb_Discovery.WhoAmIResponse{
		Operation: discoveryOperationOK(&emptypb.Empty{}),
	}, nil
}

func (s *dynamicDiscoveryServer) currentEndpoints() []*Ydb_Discovery.EndpointInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nodeIDs := append([]uint32(nil), s.nodeIDs...)

	return mockDiscoveryEndpoints(s.host, s.port, nodeIDs)
}

func (s *dynamicDiscoveryServer) setNodeIDs(nodeIDs []uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeIDs = append([]uint32(nil), nodeIDs...)
}

func (s *dynamicDiscoveryServer) endpoint() string {
	return net.JoinHostPort(s.host, strconv.FormatUint(uint64(s.port), 10))
}

func (s *dynamicDiscoveryServer) Close() {
	s.grpcServer.Stop()
	_ = s.listener.Close()
}

func (s *dynamicDiscoveryServer) activeGRPCConns() int64 {
	return s.activeConns.Load()
}

type serverConnStats struct {
	active *atomic.Int64
}

func (h *serverConnStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *serverConnStats) HandleRPC(context.Context, stats.RPCStats) {}

func (h *serverConnStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *serverConnStats) HandleConn(_ context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		h.active.Add(1)
	case *stats.ConnEnd:
		h.active.Add(-1)
	}
}

func startDynamicDiscoveryServer(tb testing.TB, nodeIDs []uint32) *dynamicDiscoveryServer {
	tb.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(tb.Context(), "tcp", "127.0.0.1:0")
	require.NoError(tb, err)

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(tb, err)

	portParsed, err := strconv.ParseUint(portStr, 10, 32)
	require.NoError(tb, err)

	srv := &dynamicDiscoveryServer{
		listener: lis,
		host:     host,
		port:     uint32(portParsed),
		nodeIDs:  append([]uint32(nil), nodeIDs...),
	}

	statsHandler := &serverConnStats{active: &srv.activeConns}
	srv.grpcServer = grpc.NewServer(grpc.StatsHandler(statsHandler))

	Ydb_Discovery_V1.RegisterDiscoveryServiceServer(srv.grpcServer, &dynamicDiscoveryService{srv: srv})

	go func() {
		_ = srv.grpcServer.Serve(lis)
	}()

	tb.Cleanup(srv.Close)

	require.Eventually(tb, func() bool {
		var d net.Dialer
		c, err := d.DialContext(tb.Context(), "tcp", lis.Addr().String())
		if err != nil {
			return false
		}
		_ = c.Close()

		return true
	}, time.Second, 10*time.Millisecond)

	return srv
}

func mockDiscoveryEndpoints(host string, port uint32, nodeIDs []uint32) []*Ydb_Discovery.EndpointInfo {
	endpoints := make([]*Ydb_Discovery.EndpointInfo, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		endpoints[i] = &Ydb_Discovery.EndpointInfo{
			Address:    host,
			Port:       port,
			LoadFactor: 0,
			Ssl:        false,
			NodeId:     nodeID,
			IpV4:       []string{host},
		}
	}

	return endpoints
}

func discoveryOperationOK(msg proto.Message) *Ydb_Operations.Operation {
	result := &anypb.Any{}
	if err := result.MarshalFrom(msg); err != nil {
		panic(err)
	}

	return &Ydb_Operations.Operation{
		Ready:  true,
		Status: Ydb.StatusIds_SUCCESS,
		Result: result,
	}
}

type connLifeEvents struct {
	mu     sync.Mutex
	dialed map[uint32]int
	parked map[uint32]int
	closed map[uint32]int
}

func newConnLifeEvents() *connLifeEvents {
	return &connLifeEvents{
		dialed: make(map[uint32]int),
		parked: make(map[uint32]int),
		closed: make(map[uint32]int),
	}
}

func (e *connLifeEvents) driverTrace() *trace.Driver {
	return &trace.Driver{
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(done trace.DriverConnDialDoneInfo) {
				if done.Error != nil {
					return
				}
				e.mu.Lock()
				e.dialed[nodeID]++
				e.mu.Unlock()
			}
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(trace.DriverConnCloseDoneInfo) {
				e.mu.Lock()
				e.closed[nodeID]++
				e.mu.Unlock()
			}
		},
		OnConnPark: func(info trace.DriverConnParkStartInfo) func(trace.DriverConnParkDoneInfo) {
			nodeID := info.Endpoint.NodeID()

			return func(done trace.DriverConnParkDoneInfo) {
				if done.Error != nil {
					return
				}
				e.mu.Lock()
				e.parked[nodeID]++
				e.mu.Unlock()
			}
		},
	}
}

func (e *connLifeEvents) dialedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.dialed[nodeID]
}

func (e *connLifeEvents) closedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.closed[nodeID]
}

func (e *connLifeEvents) parkedCount(nodeID uint32) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.parked[nodeID]
}

func dialWhoAmI(tb testing.TB, b *Balancer, nodeID uint32) {
	tb.Helper()

	ctx := endpoint.WithNodeID(tb.Context(), nodeID)
	reply := &Ydb_Discovery.WhoAmIResponse{}

	err := b.Invoke(ctx, Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName, &Ydb_Discovery.WhoAmIRequest{}, reply)
	require.NoError(tb, err)
}

func connByNodeID(b *Balancer, nodeID uint32) conn.Conn {
	for _, cc := range b.connections().All() {
		if cc.Endpoint().NodeID() == nodeID {
			return cc
		}
	}

	return nil
}

func activeNodeIDs(b *Balancer) []uint32 {
	conns := b.connections().All()
	ids := make([]uint32, len(conns))
	for i, cc := range conns {
		ids[i] = cc.Endpoint().NodeID()
	}

	return ids
}

func connInQuarantine(b *Balancer, nodeID uint32) conn.Conn {
	if state := b.connectionsState.Load(); state != nil {
		for _, cc := range state.quarantine {
			if cc.Endpoint().NodeID() == nodeID {
				return cc
			}
		}
	}

	return nil
}

// TestBalancerDiscoveryDropClosesGRPC verifies end-to-end that a node removed from
// ListEndpoints eventually closes its pooled gRPC connection after the quarantine
// cycle (two discovery rounds after the drop), and Balancer.Close closes the rest.
func TestBalancerDiscoveryDropClosesGRPC(t *testing.T) {
	const (
		node1 uint32 = 1
		node2 uint32 = 2
	)

	ctx := t.Context()
	srv := startDynamicDiscoveryServer(t, []uint32{node1, node2})
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(srv.endpoint()),
		config.WithDatabase("/local"),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(&balancerConfig.Config{}),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	b, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	require.ElementsMatch(t, []uint32{node1, node2}, activeNodeIDs(b))

	dialWhoAmI(t, b, node1)
	dialWhoAmI(t, b, node2)

	require.Equal(t, 1, events.dialedCount(node1))
	require.Equal(t, 1, events.dialedCount(node2))
	require.GreaterOrEqual(t, srv.activeGRPCConns(), int64(2))

	node2Conn := connByNodeID(b, node2)
	require.NotNil(t, node2Conn)
	require.Equal(t, state.Online, node2Conn.State())

	// Discovery #2: same cluster — quarantine cycle, refs increment.
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.ElementsMatch(t, []uint32{node1, node2}, activeNodeIDs(b))
	require.Equal(t, 0, events.closedCount(node2))

	// Discovery #3: node 2 disappears — moved to quarantine, gRPC still alive.
	srv.setNodeIDs([]uint32{node1})
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.Equal(t, []uint32{node1}, activeNodeIDs(b))
	require.Equal(t, 0, events.closedCount(node2), "gRPC must not close on first discovery after drop")

	quarantined := connInQuarantine(b, node2)
	require.NotNil(t, quarantined, "dropped node must remain in quarantine")
	require.Same(t, node2Conn, quarantined)

	// Discovery #4: same cluster — quarantine released, dropped node gRPC must close.
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.Equal(t, []uint32{node1}, activeNodeIDs(b))

	require.Eventually(t, func() bool {
		return events.closedCount(node2) == 1 && node2Conn.State() == state.Destroyed
	}, time.Second, 10*time.Millisecond, "node removed from discovery must close gRPC connection")

	require.Equal(t, 0, events.closedCount(node1))

	// Balancer.Close: release active + quarantine, remaining pooled conns must close.
	node1Conn := connByNodeID(b, node1)
	require.NotNil(t, node1Conn)
	require.Equal(t, 0, events.closedCount(node1))

	require.NoError(t, b.Close(ctx))

	require.Eventually(t, func() bool {
		return events.closedCount(node1) == 1 && node1Conn.State() == state.Destroyed
	}, time.Second, 10*time.Millisecond, "balancer close must close remaining active connections")

	require.Equal(t, 1, events.closedCount(node2))
}

func TestBalancerDiscoveryDropDestroysParkedConnection(t *testing.T) {
	const (
		node1 uint32 = 1
		node2 uint32 = 2
		ttl          = 50 * time.Millisecond
	)

	ctx := t.Context()
	srv := startDynamicDiscoveryServer(t, []uint32{node1, node2})
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(srv.endpoint()),
		config.WithDatabase("/local"),
		config.WithConnectionTTL(ttl),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(&balancerConfig.Config{}),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	b, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	dialWhoAmI(t, b, node2)
	node2Conn := connByNodeID(b, node2)
	require.NotNil(t, node2Conn)

	require.Eventually(t, func() bool {
		return node2Conn.State() == state.Offline && events.parkedCount(node2) == 1
	}, time.Second, 10*time.Millisecond, "connection TTL must park the node transport")
	require.Equal(t, 0, events.closedCount(node2), "parking must preserve the pooled wrapper")

	// Discovery #2: keep both nodes and establish the quarantine generation.
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.Same(t, node2Conn, connByNodeID(b, node2))
	require.Equal(t, state.Offline, node2Conn.State())

	// Discovery #3: node 2 disappears but remains referenced by quarantine.
	srv.setNodeIDs([]uint32{node1})
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.Same(t, node2Conn, connInQuarantine(b, node2))
	require.Equal(t, state.Offline, node2Conn.State())
	require.Equal(t, 0, events.closedCount(node2))

	// Discovery #4: release quarantine and destroy the already parked wrapper.
	require.NoError(t, b.clusterDiscoveryAttemptWithDial(ctx))
	require.Eventually(t, func() bool {
		return node2Conn.State() == state.Destroyed && events.closedCount(node2) == 1
	}, time.Second, 10*time.Millisecond, "Discovery removal must destroy a parked connection")
	require.Equal(t, 1, events.parkedCount(node2))
	require.Equal(t, 1, events.dialedCount(node2))

	require.NoError(t, b.Close(ctx))
}

func TestBalancerConnectionTTLParksTransportsAfterNetworkLoss(t *testing.T) {
	const (
		nodeCount = 16
		ttl       = 50 * time.Millisecond
	)

	nodeIDs := make([]uint32, nodeCount)
	for i := range nodeIDs {
		nodeIDs[i] = uint32(i + 1)
	}

	ctx := t.Context()
	srv := startDynamicDiscoveryServer(t, nodeIDs)
	events := newConnLifeEvents()

	cfg := config.New(
		config.WithEndpoint(srv.endpoint()),
		config.WithDatabase("/local"),
		config.WithConnectionTTL(ttl),
		config.WithDialTimeout(ttl),
		config.WithGrpcOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		config.WithTrace(*events.driverTrace()),
		config.WithBalancer(&balancerConfig.Config{}),
	)

	pool := conn.NewPool(ctx, cfg)
	defer func() {
		require.NoError(t, pool.RemoveRef(ctx))
	}()

	b, err := New(ctx, cfg, pool, discoveryConfig.WithInterval(0))
	require.NoError(t, err)

	dialWhoAmI(t, b, nodeIDs[0])
	srv.Close()

	for _, nodeID := range nodeIDs[1:] {
		callCtx, cancel := context.WithTimeout(endpoint.WithNodeID(ctx, nodeID), ttl)
		reply := &Ydb_Discovery.WhoAmIResponse{}
		err = b.Invoke(
			callCtx,
			Ydb_Discovery_V1.DiscoveryService_WhoAmI_FullMethodName,
			&Ydb_Discovery.WhoAmIRequest{},
			reply,
		)
		cancel()
		require.Error(t, err)
	}

	for _, nodeID := range nodeIDs {
		require.Equal(t, 1, events.dialedCount(nodeID))
	}

	require.Eventually(t, func() bool {
		for _, nodeID := range nodeIDs {
			cc := connByNodeID(b, nodeID)
			if cc == nil || cc.State() != state.Offline || events.parkedCount(nodeID) != 1 {
				return false
			}
		}

		return true
	}, 5*time.Second, 10*time.Millisecond)

	require.ElementsMatch(t, nodeIDs, activeNodeIDs(b), "parking must keep Discovery wrappers in the balancer")
	require.NoError(t, b.Close(ctx))

	for _, nodeID := range nodeIDs {
		require.Equal(t, 1, events.closedCount(nodeID))
	}
}
