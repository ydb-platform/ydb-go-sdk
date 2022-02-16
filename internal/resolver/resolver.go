package resolver

import "google.golang.org/grpc/resolver"

type dnsBuilder struct {
	builder resolver.Builder
	scheme  string
}

type dnsResolver struct {
	resolver resolver.Resolver
}

func (r *dnsResolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.resolver.ResolveNow(options)
}

func (r *dnsResolver) Close() {
	r.resolver.Close()
}

func (d *dnsBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	resolver, err := d.builder.Build(target, cc, opts)
	if err != nil {
		return nil, err
	}
	return &dnsResolver{
		resolver: resolver,
	}, nil
}

func (d *dnsBuilder) Scheme() string {
	return d.scheme
}

func New(scheme string) resolver.Builder {
	return &dnsBuilder{
		builder: resolver.Get("dns"),
		scheme:  scheme,
	}
}
