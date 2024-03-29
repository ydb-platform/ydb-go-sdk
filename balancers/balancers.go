package balancers

import (
	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Deprecated: RoundRobin is RandomChoice now
func RoundRobin() *balancerConfig.Config {
	return balancerConfig.New()
}

func RandomChoice() *balancerConfig.Config {
	return balancerConfig.New()
}

func SingleConn() *balancerConfig.Config {
	return balancerConfig.New(balancerConfig.UseSingleConn())
}

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferLocalDC balancer try to autodetect local DC from client side.
func PreferLocalDC(balancer *balancerConfig.Config) *balancerConfig.Config {
	return balancer.With(
		balancerConfig.FilterLocalDC(),
		balancerConfig.DetectLocalDC(),
	)
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer *balancerConfig.Config) *balancerConfig.Config {
	return PreferLocalDC(balancer).With(balancerConfig.AllowFallback())
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "ABC", "DEF", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	return balancer.With(balancerConfig.FilterLocations(locations...))
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	return balancer.With(
		balancerConfig.FilterLocations(locations...),
		balancerConfig.AllowFallback(),
	)
}

type Endpoint interface {
	NodeID() uint32
	Address() string
	Location() string
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	return balancer.With(
		balancerConfig.FilterFunc(func(_ balancerConfig.Info, c conn.Info) bool {
			return filter(c.Endpoint())
		}),
	)
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	return balancer.With(
		balancerConfig.FilterFunc(func(_ balancerConfig.Info, c conn.Info) bool {
			return filter(c.Endpoint())
		}),
		balancerConfig.AllowFallback(),
	)
}

// Default balancer used by default
func Default() *balancerConfig.Config {
	return balancerConfig.New()
}
