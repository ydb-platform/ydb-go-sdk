package xresolver

import (
	"strings"

	"google.golang.org/grpc/resolver"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type dnsBuilder struct {
	resolver.Builder

	scheme        string
	trace         *trace.Driver
	addressFilter func(addr string) bool
}

type clientConn struct {
	resolver.ClientConn

	target        resolver.Target
	trace         *trace.Driver
	addressFilter func(addr string) bool
}

func (c *clientConn) Endpoint() string {
	endpoint := c.target.URL.Path
	if endpoint == "" {
		endpoint = c.target.URL.Opaque
	}

	return strings.TrimPrefix(endpoint, "/")
}

func (c *clientConn) UpdateState(state resolver.State) (err error) {
	if c.addressFilter != nil {
		filtered := make([]resolver.Address, 0, len(state.Addresses))
		for _, addr := range state.Addresses {
			if c.addressFilter(addr.Addr) {
				filtered = append(filtered, addr)
			}
		}
		state.Addresses = filtered
	}

	onDone := trace.DriverOnResolve(c.trace,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/xresolver.(*clientConn).UpdateState"),
		c.Endpoint(), func() (addrs []string) {
			for i := range state.Addresses {
				addrs = append(addrs, state.Addresses[i].Addr)
			}

			return
		}(),
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

func (d *dnsBuilder) Build(
	target resolver.Target, //nolint:gocritic
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	return d.Builder.Build(target, &clientConn{
		ClientConn:    cc,
		target:        target,
		trace:         d.trace,
		addressFilter: d.addressFilter,
	}, opts)
}

func (d *dnsBuilder) Scheme() string {
	return d.scheme
}

// New creates a gRPC resolver.Builder that wraps the standard "dns" resolver.
//
// If addressFilter is non-nil, it is called for every resolved address before
// the address list is forwarded to the gRPC connection manager. Addresses for
// which addressFilter returns false are dropped. All addresses that pass the
// filter are kept, so connection round-robin behaviour is preserved.
func New(scheme string, trace *trace.Driver, addressFilter func(addr string) bool) resolver.Builder {
	return &dnsBuilder{
		Builder:       resolver.Get("dns"),
		scheme:        scheme,
		trace:         trace,
		addressFilter: addressFilter,
	}
}
