package resolver

import (
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type dnsBuilder struct {
	builder resolver.Builder
	scheme  string
	trace   trace.Driver
}

type clientConn struct {
	target resolver.Target
	cc     resolver.ClientConn
	trace  trace.Driver
}

func (c *clientConn) UpdateState(state resolver.State) (err error) {
	onDone := trace.DriverOnResolve(
		c.trace,
		// nolint:staticcheck
		// nolint:nolintlint
		c.target.Endpoint,
		func() (addrs []string) {
			for _, a := range state.Addresses {
				addrs = append(addrs, a.Addr)
			}
			return
		}(),
	)
	defer func() {
		onDone(err)
	}()
	err = c.cc.UpdateState(state)
	if err != nil {
		err = errors.Errorf(0, "clientConn.UpdateState(%v): %w", state, err)
	}
	return err
}

func (c *clientConn) ReportError(err error) {
	c.cc.ReportError(err)
}

func (c *clientConn) NewAddress(addresses []resolver.Address) {
	// nolint:staticcheck
	// nolint:nolintlint
	c.cc.NewAddress(addresses)
}

func (c *clientConn) NewServiceConfig(serviceConfig string) {
	// nolint:staticcheck
	// nolint:nolintlint
	c.cc.NewServiceConfig(serviceConfig)
}

func (c *clientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return c.cc.ParseServiceConfig(serviceConfigJSON)
}

func (d *dnsBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	return d.builder.Build(
		target,
		&clientConn{
			target: target,
			cc:     cc,
			trace:  d.trace,
		},
		opts,
	)
}

func (d *dnsBuilder) Scheme() string {
	return d.scheme
}

func New(scheme string, trace trace.Driver) resolver.Builder {
	return &dnsBuilder{
		builder: resolver.Get("dns"),
		scheme:  scheme,
		trace:   trace,
	}
}
