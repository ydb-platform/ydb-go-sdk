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

// Plan contains endpoint-source and location-resolution decisions compiled from an estimator tree.
// Endpoint policy itself remains represented by the immutable Estimator interface.
type Plan struct {
	estimator Estimator
	source    endpointSource
	resolver  locationResolver
}

// Compile extracts lifecycle settings from an estimator tree.
func Compile(estimator Estimator) Plan {
	return compile(normalize(estimator))
}

// Estimator returns the configured endpoint estimator tree.
func (p Plan) Estimator() Estimator {
	return normalize(p.estimator)
}

// Start starts the endpoint source selected by the estimator tree.
func (p Plan) Start(ctx context.Context, runtime Runtime) (Controller, error) {
	if p.source == nil {
		return clusterEndpointSource{}.Start(ctx, runtime)
	}

	return p.source.Start(ctx, runtime)
}

// ResolveLocation resolves the location used by endpoint estimators.
func (p Plan) ResolveLocation(
	ctx context.Context,
	endpoints []endpoint.Endpoint,
	discoveredLocation string,
	detector LocalDCDetector,
) (ResolvedLocation, error) {
	if p.resolver == nil {
		return discoveredLocationResolver{}.Resolve(ctx, endpoints, discoveredLocation, detector)
	}

	return p.resolver.Resolve(ctx, endpoints, discoveredLocation, detector)
}

type compiler interface {
	compile() Plan
}

func compile(estimator Estimator) Plan {
	if c, ok := estimator.(compiler); ok {
		return c.compile()
	}

	return Plan{
		estimator: estimator,
		source:    clusterEndpointSource{},
		resolver:  discoveredLocationResolver{},
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
		estimator: b,
		source:    clusterEndpointSource{},
		resolver:  discoveredLocationResolver{},
	}
}

func (b singleConn) compile() Plan {
	return Plan{
		estimator: b,
		source:    configuredEndpointSource{},
		resolver:  discoveredLocationResolver{},
	}
}

func (p prefer) compile() Plan {
	plan := compile(p.child)
	plan.estimator = p

	return plan
}

func (n nearestDC) compile() Plan {
	plan := compile(n.child)
	plan.estimator = n
	plan.resolver = nearestDCLocationResolver{}

	return plan
}
