package xresolver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fakeClientConn struct {
	resolver.ClientConn

	state resolver.State
}

func (c *fakeClientConn) UpdateState(state resolver.State) error {
	c.state = state

	return nil
}

func TestClientConnUpdateStateFiltersAddresses(t *testing.T) {
	fakeConn := &fakeClientConn{}
	resolverConn := &clientConn{
		ClientConn: fakeConn,
		trace:      &trace.Driver{},
		filter:     balancerConfig.IPv6.AddressFilter(),
	}

	err := resolverConn.UpdateState(resolver.State{Addresses: []resolver.Address{
		{Addr: "192.0.2.1:2135"},
		{Addr: "[2001:db8::1]:2135"},
		{Addr: "[2001:db8::2]:2135"},
		{Addr: "[::ffff:192.0.2.2]:2135"},
		{Addr: "not-an-ip:2135"},
	}, Endpoints: []resolver.Endpoint{
		{Addresses: []resolver.Address{{Addr: "192.0.2.3:2135"}}},
		{Addresses: []resolver.Address{{Addr: "[2001:db8::3]:2135"}}},
	}})
	require.NoError(t, err)
	require.Equal(t, []resolver.Address{
		{Addr: "[2001:db8::1]:2135"},
		{Addr: "[2001:db8::2]:2135"},
	}, fakeConn.state.Addresses)
	require.Equal(t, []resolver.Endpoint{
		{Addresses: []resolver.Address{{Addr: "[2001:db8::3]:2135"}}},
	}, fakeConn.state.Endpoints)
}

func TestTarget(t *testing.T) {
	require.Equal(t, "ydb:///example.com:2135", Target("example.com:2135"))
	require.Equal(t, "ydb:///%5B2001:db8::1%5D:2135", Target("[2001:db8::1]:2135"))
	require.Equal(t, Scheme, New(&trace.Driver{}, nil).Scheme())
}
