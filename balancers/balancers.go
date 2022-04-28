package balancers

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/rr"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func RoundRobin() balancer.Balancer {
	return rr.RoundRobin()
}

func RandomChoice() balancer.Balancer {
	return rr.RandomChoice()
}

func SingleConn() balancer.Balancer {
	return single.Balancer()
}

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocalDC(balancer balancer.Balancer) balancer.Balancer {
	return Prefer(
		balancer,
		func(endpoint Endpoint) bool {
			return endpoint.LocalDC()
		},
	)
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer balancer.Balancer) balancer.Balancer {
	return PreferWithFallback(
		balancer,
		func(endpoint Endpoint) bool {
			return endpoint.LocalDC()
		},
	)
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "ABC", "DEF", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer balancer.Balancer, locations ...string) balancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return Prefer(
		balancer,
		func(endpoint Endpoint) bool {
			location := strings.ToUpper(endpoint.Location())
			for _, l := range locations {
				if location == l {
					return true
				}
			}
			return false
		},
	)
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer balancer.Balancer, locations ...string) balancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return PreferWithFallback(
		balancer,
		func(endpoint Endpoint) bool {
			location := strings.ToUpper(endpoint.Location())
			for _, l := range locations {
				if location == l {
					return true
				}
			}
			return false
		},
	)
}

type Endpoint interface {
	NodeID() uint32
	Address() string
	Location() string
	LocalDC() bool
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer balancer.Balancer, filter func(endpoint Endpoint) bool) balancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			balancer,
			func(cc conn.Conn) bool {
				return filter(cc.Endpoint())
			},
		),
	)
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer balancer.Balancer, filter func(endpoint Endpoint) bool) balancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			balancer,
			func(cc conn.Conn) bool {
				return filter(cc.Endpoint())
			},
		),
		multi.WithBalancer(
			balancer,
			func(cc conn.Conn) bool {
				return !filter(cc.Endpoint())
			},
		),
	)
}

// Default balancer used by default
func Default() balancer.Balancer {
	return RandomChoice()
}
