package strategy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// Controller manages the endpoint source lifecycle. Static sources may return nil.
type Controller interface {
	Force()
	Stop()
}

// Runtime provides operations needed to start an endpoint source.
type Runtime interface {
	StartClusterDiscovery(ctx context.Context) (Controller, error)
	UseConfiguredEndpoint(ctx context.Context) (Controller, error)
}

// ResolvedLocation carries location selection data and trace metadata.
type ResolvedLocation struct {
	SelfLocation string
	NeedLocalDC  bool
}

// LocalDCDetector detects the nearest data center for discovered endpoints.
type LocalDCDetector func(ctx context.Context, endpoints []endpoint.Endpoint) (string, error)

// Plan is an executable representation of a balancer tree.
type Plan struct {
	balancer Balancer
	source   endpointSource
	resolver locationResolver
}

// Compile converts a balancer tree into endpoint selection and lifecycle behavior.
func Compile(balancer Balancer) Plan {
	return compile(normalize(balancer))
}

// Balancer returns the endpoint selection part of the plan.
func (p Plan) Balancer() Balancer {
	return p.balancer
}

// Start starts the endpoint source selected by the balancer tree.
func (p Plan) Start(ctx context.Context, runtime Runtime) (Controller, error) {
	return p.source.Start(ctx, runtime)
}

// ResolveLocation resolves the location used by endpoint filters.
func (p Plan) ResolveLocation(
	ctx context.Context,
	endpoints []endpoint.Endpoint,
	discoveredLocation string,
	detector LocalDCDetector,
) (ResolvedLocation, error) {
	return p.resolver.Resolve(ctx, endpoints, discoveredLocation, detector)
}

type compiler interface {
	compile() Plan
}

func compile(balancer Balancer) Plan {
	if c, ok := balancer.(compiler); ok {
		return c.compile()
	}

	return Plan{
		balancer: balancer,
		source:   clusterEndpointSource{},
		resolver: discoveredLocationResolver{},
	}
}

type endpointSource interface {
	Start(ctx context.Context, runtime Runtime) (Controller, error)
}

type clusterEndpointSource struct{}

func (clusterEndpointSource) Start(ctx context.Context, runtime Runtime) (Controller, error) {
	return runtime.StartClusterDiscovery(ctx)
}

type configuredEndpointSource struct{}

func (configuredEndpointSource) Start(ctx context.Context, runtime Runtime) (Controller, error) {
	return runtime.UseConfiguredEndpoint(ctx)
}

type locationResolver interface {
	Resolve(
		ctx context.Context,
		endpoints []endpoint.Endpoint,
		discoveredLocation string,
		detector LocalDCDetector,
	) (ResolvedLocation, error)
}

type discoveredLocationResolver struct{}

func (discoveredLocationResolver) Resolve(
	_ context.Context,
	_ []endpoint.Endpoint,
	discoveredLocation string,
	_ LocalDCDetector,
) (ResolvedLocation, error) {
	return ResolvedLocation{SelfLocation: discoveredLocation}, nil
}

type nearestDCLocationResolver struct{}

func (nearestDCLocationResolver) Resolve(
	ctx context.Context,
	endpoints []endpoint.Endpoint,
	_ string,
	detector LocalDCDetector,
) (ResolvedLocation, error) {
	location, err := detector(ctx, endpoints)
	if err != nil {
		return ResolvedLocation{}, err
	}

	return ResolvedLocation{
		SelfLocation: location,
		NeedLocalDC:  true,
	}, nil
}

func (b randomChoice) compile() Plan {
	return Plan{
		balancer: b,
		source:   clusterEndpointSource{},
		resolver: discoveredLocationResolver{},
	}
}

func (b singleConn) compile() Plan {
	return Plan{
		balancer: b,
		source:   configuredEndpointSource{},
		resolver: discoveredLocationResolver{},
	}
}

func (p prefer) compile() Plan {
	plan := compile(p.child)
	plan.balancer = p

	return plan
}

func (n nearestDC) compile() Plan {
	plan := compile(n.Balancer)
	plan.balancer = n
	plan.resolver = nearestDCLocationResolver{}

	return plan
}
