package resolver

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"google.golang.org/grpc/resolver"
)

type dnsBuilder struct {
	resolver.Builder
	scheme string
	trace  trace.Driver
}

type clientConn struct {
	resolver.ClientConn
	target resolver.Target
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
	err = c.ClientConn.UpdateState(state)
	if err != nil {
		err = errors.WithStackTrace(err)
	}
	return err
}

func (d *dnsBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	return d.Build(
		target,
		&clientConn{
			ClientConn: cc,
			target:     target,
			trace:      d.trace,
		},
		opts,
	)
}

func (d *dnsBuilder) Scheme() string {
	return d.scheme
}

func New(scheme string, trace trace.Driver) resolver.Builder {
	return &dnsBuilder{
		Builder: resolver.Get("dns"),
		scheme:  scheme,
		trace:   trace,
	}
}
