package xresolver

import (
	"net/url"
	"strings"

	"google.golang.org/grpc/resolver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const Scheme = "ydb"

type dnsBuilder struct {
	resolver.Builder

	trace  *trace.Driver
	filter func(string) bool
}

type clientConn struct {
	resolver.ClientConn

	target resolver.Target
	trace  *trace.Driver
	filter func(string) bool
}

func Target(endpoint string) string {
	return (&url.URL{
		Scheme: Scheme,
		Path:   "/" + endpoint,
	}).String()
}

func (c *clientConn) Endpoint() string {
	endpoint := c.target.URL.Path
	if endpoint == "" {
		endpoint = c.target.URL.Opaque
	}

	return strings.TrimPrefix(endpoint, "/")
}

func (c *clientConn) UpdateState(state resolver.State) (err error) {
	if c.filter != nil {
		state.Addresses = filterAddresses(state.Addresses, c.filter)
		state.Endpoints = filterEndpoints(state.Endpoints, c.filter)
	}

	onDone := gtrace.DriverOnResolve(c.trace,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver.(*clientConn).UpdateState"),
		c.Endpoint(), stateAddresses(state),
	)
	defer func() {
		onDone(err)
	}()

	err = c.ClientConn.UpdateState(state)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func filterAddresses(addresses []resolver.Address, filter func(string) bool) []resolver.Address {
	if len(addresses) == 0 {
		return addresses
	}

	filtered := make([]resolver.Address, 0, len(addresses))
	for _, address := range addresses {
		if filter(address.Addr) {
			filtered = append(filtered, address)
		}
	}

	return filtered
}

func filterEndpoints(endpoints []resolver.Endpoint, filter func(string) bool) []resolver.Endpoint {
	if len(endpoints) == 0 {
		return endpoints
	}

	filtered := make([]resolver.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		endpoint.Addresses = filterAddresses(endpoint.Addresses, filter)
		if len(endpoint.Addresses) != 0 {
			filtered = append(filtered, endpoint)
		}
	}

	return filtered
}

func stateAddresses(state resolver.State) []string {
	addresses := make([]string, 0, len(state.Addresses))
	for _, address := range state.Addresses {
		addresses = append(addresses, address.Addr)
	}
	for _, endpoint := range state.Endpoints {
		for _, address := range endpoint.Addresses {
			addresses = append(addresses, address.Addr)
		}
	}

	return addresses
}

func (d *dnsBuilder) Build(
	target resolver.Target, //nolint:gocritic
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	return d.Builder.Build(target, &clientConn{
		ClientConn: cc,
		target:     target,
		trace:      d.trace,
		filter:     d.filter,
	}, opts)
}

func (d *dnsBuilder) Scheme() string {
	return Scheme
}

// New creates a resolver that filters the addresses returned by the DNS resolver.
func New(trace *trace.Driver, filter func(string) bool) resolver.Builder {
	return &dnsBuilder{
		Builder: resolver.Get("dns"),
		trace:   trace,
		filter:  filter,
	}
}
