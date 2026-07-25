package balancer

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestApplyDiscoveredEndpointsMaxConnections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	t.Cleanup(func() { _ = pool.RemoveRef(ctx) })

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
		balancerConfig: balancerConfig.Config{
			MaxConnections: 3,
		},
	}

	endpoints := discoveredEndpoints(20)
	b.applyDiscoveredEndpoints(ctx, endpoints, "")
	require.Len(t, b.connections().All(), 3)
	require.Len(t, b.lastDiscovered, 20)

	// Sticky: rediscovery with the same list keeps the same active set.
	first := endpointKeys(b.connections().All())
	b.applyDiscoveredEndpoints(ctx, endpoints, "")
	require.Equal(t, first, endpointKeys(b.connections().All()))

	// Drop one active endpoint from discovery — slot is refilled, size stays capped.
	active := b.connections().All()
	dropped := active[0].Endpoint()
	remaining := make([]endpoint.Endpoint, 0, len(endpoints)-1)
	for _, e := range endpoints {
		if e.Key() != dropped.Key() {
			remaining = append(remaining, e)
		}
	}
	b.applyDiscoveredEndpoints(ctx, remaining, "")
	require.Len(t, b.connections().All(), 3)
	for _, cc := range b.connections().All() {
		require.NotEqual(t, dropped.Key(), cc.Endpoint().Key())
	}
}

func TestBanEvictsFromMaxConnections(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	t.Cleanup(func() { _ = pool.RemoveRef(ctx) })

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
		balancerConfig: balancerConfig.Config{
			MaxConnections: 3,
		},
	}

	endpoints := discoveredEndpoints(10)
	b.applyDiscoveredEndpoints(ctx, endpoints, "")
	require.Len(t, b.connections().All(), 3)

	before := endpointKeys(b.connections().All())
	banned := b.connections().All()[0]
	bannedKey := banned.Endpoint().Key()
	b.handleBan(ctx, banned, fmt.Errorf("transport: %w", status.Error(codes.Unavailable, "down")))

	after := b.connections().All()
	require.Len(t, after, 3)
	for _, cc := range after {
		require.NotEqual(t, bannedKey, cc.Endpoint().Key())
	}
	require.NotEqual(t, before, endpointKeys(after))
	// Ban + eviction releases the discovery ref and closes gRPC (Offline/Destroyed),
	// so the connection no longer occupies a MaxConnections slot.
	require.NotEqual(t, state.Online, banned.State())
	require.NotEqual(t, state.Created, banned.State())
}

func TestPinOutsideActiveSetSoftExceedsLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cfg := config.New()
	pool := conn.NewPool(ctx, cfg)
	t.Cleanup(func() { _ = pool.RemoveRef(ctx) })

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
		balancerConfig: balancerConfig.Config{
			MaxConnections: 2,
		},
	}

	endpoints := discoveredEndpoints(10)
	b.applyDiscoveredEndpoints(ctx, endpoints, "")
	require.Len(t, b.connections().All(), 2)

	activeKeys := endpointKeys(b.connections().All())
	var outside endpoint.Endpoint
	for _, e := range endpoints {
		if _, ok := activeKeys[e.Key()]; !ok {
			outside = e

			break
		}
	}
	require.NotNil(t, outside)

	cc, err := b.nextConn(endpoint.WithNodeID(ctx, outside.NodeID(), endpoint.WithFallback(false)))
	require.NoError(t, err)
	require.Equal(t, outside.Key(), cc.Endpoint().Key())
	require.Greater(t, len(b.connections().All()), 2)
}

// TestMaxConnectionsLimitsGrpcCallbackSerializerGoroutines reproduces the class
// of leaks reported after connParker removal: with many discovered endpoints,
// dialing unreachable nodes leaves gRPC CallbackSerializer goroutines alive for
// each ClientConn. Capping MaxConnections bounds how many such dials the
// balancer keeps, so goroutine growth stays limited.
func TestMaxConnectionsLimitsGrpcCallbackSerializerGoroutines(t *testing.T) {
	t.Parallel()

	const (
		endpointCount = 40
		maxConns      = 5
	)

	withoutLimit := countCallbackSerializersAfterDialStorm(t, endpointCount, 0)
	withLimit := countCallbackSerializersAfterDialStorm(t, endpointCount, maxConns)

	t.Logf("CallbackSerializer goroutines: unlimited=%d maxConnections=%d -> %d",
		withoutLimit, maxConns, withLimit)

	require.Greater(t, withoutLimit, withLimit,
		"without MaxConnections dial storm should leave more CallbackSerializer goroutines")
	require.LessOrEqual(t, withLimit, maxConns*4+15,
		"with MaxConnections CallbackSerializer count should stay near the active set size")
}

func countCallbackSerializersAfterDialStorm(tb testing.TB, endpointCount, maxConnections int) int {
	tb.Helper()

	ctx := context.Background()
	cfg := config.New(
		config.WithDialTimeout(40*time.Millisecond),
		config.WithGrpcOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				<-ctx.Done()

				return nil, ctx.Err()
			}),
		),
	)
	pool := conn.NewPool(ctx, cfg)
	tb.Cleanup(func() { _ = pool.RemoveRef(ctx) })

	b := &Balancer{
		driverConfig: cfg,
		pool:         pool,
		balancerConfig: balancerConfig.Config{
			MaxConnections: maxConnections,
		},
	}

	endpoints := discoveredEndpoints(endpointCount)
	b.applyDiscoveredEndpoints(ctx, endpoints, "")
	if maxConnections > 0 {
		require.Len(tb, b.connections().All(), maxConnections)
	} else {
		require.Len(tb, b.connections().All(), endpointCount)
	}

	before := countGoroutinesWith("CallbackSerializer")

	// Dial every active connection; unreachable dialer leaves ClientConn around
	// until Close — without a cap that multiplies CallbackSerializer goroutines.
	for _, cc := range b.connections().All() {
		invokeCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
		_ = cc.Invoke(invokeCtx, "/ydb.Discovery.V1.DiscoveryService/ListEndpoints", nil, nil)
		cancel()
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	after := countGoroutinesWith("CallbackSerializer")
	grown := after - before
	if grown < 0 {
		grown = after
	}

	return grown
}

func endpointKeys(conns []conn.Conn) map[endpoint.Key]struct{} {
	keys := make(map[endpoint.Key]struct{}, len(conns))
	for _, cc := range conns {
		keys[cc.Endpoint().Key()] = struct{}{}
	}

	return keys
}

func countGoroutinesWith(substr string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]

			break
		}
		buf = make([]byte, 2*len(buf))
	}

	return strings.Count(string(buf), substr)
}
