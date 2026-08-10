package balancers

import (
	"slices"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Balancer describes an immutable, composable endpoint-selection strategy.
type Balancer = strategy.Balancer

// Deprecated: RoundRobin is an alias to RandomChoice now
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func RoundRobin() Balancer {
	return RandomChoice()
}

func RandomChoice() Balancer {
	return strategy.RandomChoice()
}

func SingleConn() Balancer {
	return strategy.SingleConn()
}

// WithMaxConnections sets the maximum number of discovered endpoints kept in
// the active connection set. Existing healthy endpoints are preferred across
// discovery updates and banned endpoints are replaced.
//
// The limit is soft: [WithNodeID] and session or stream affinity may require a
// connection to an endpoint outside the active set.
//
// Zero and negative values mean unlimited.
func WithMaxConnections(balancer Balancer, limit int) Balancer {
	return strategy.WithMaxConnections(balancer, limit)
}

type filterLocalDC struct{}

func (filterLocalDC) Allow(info strategy.Info, e endpoint.Info) bool {
	return e.Location() == info.SelfLocation
}

func (filterLocalDC) String() string {
	return "LocalDC"
}

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(balancer Balancer) Balancer {
	return PreferNearestDC(balancer)
}

// PreferNearestDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferNearestDC balancer try to autodetect local DC from client side.
func PreferNearestDC(balancer Balancer) Balancer {
	return strategy.Prefer(balancer, filterLocalDC{}, false, true)
}

// Deprecated: use PreferNearestDCWithFallBack instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(balancer Balancer) Balancer {
	return PreferNearestDCWithFallBack(balancer)
}

// PreferNearestDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferNearestDCWithFallBack(balancer Balancer) Balancer {
	return strategy.Prefer(balancer, filterLocalDC{}, true, true)
}

// PreferNearestDCWithFallback is an alias for [PreferNearestDCWithFallBack].
func PreferNearestDCWithFallback(balancer Balancer) Balancer {
	return PreferNearestDCWithFallBack(balancer)
}

type filterLocations []string

func (locations filterLocations) Allow(_ strategy.Info, e endpoint.Info) bool {
	location := strings.ToUpper(e.Location())

	return slices.Contains(locations, location)
}

func (locations filterLocations) String() string {
	buffer := xstring.Buffer()
	defer buffer.Free()

	buffer.WriteString("Locations{")
	for i, l := range locations {
		if i != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteString(l)
	}
	buffer.WriteByte('}')

	return buffer.String()
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "ABC", "DEF", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer Balancer, locations ...string) Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	// Prevent modify source locations
	locations = slices.Clone(locations)

	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return strategy.Prefer(balancer, filterLocations(locations), false, false)
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer Balancer, locations ...string) Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	locations = slices.Clone(locations)
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return strategy.Prefer(balancer, filterLocations(locations), true, false)
}

type Endpoint interface {
	NodeID() uint32
	Address() string
	Location() string

	// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
	// It work good only if connection url always point to local dc.
	// Will be removed after Oct 2024.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	LocalDC() bool
}

type filterFunc func(info strategy.Info, e endpoint.Info) bool

func (p filterFunc) Allow(info strategy.Info, e endpoint.Info) bool {
	return p(info, e)
}

func (p filterFunc) String() string {
	return "Custom"
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer Balancer, filter func(endpoint Endpoint) bool) Balancer {
	return strategy.Prefer(balancer, filterFunc(func(_ strategy.Info, e endpoint.Info) bool {
		return filter(e)
	}), false, false)
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer Balancer, filter func(endpoint Endpoint) bool) Balancer {
	return strategy.Prefer(balancer, filterFunc(func(_ strategy.Info, e endpoint.Info) bool {
		return filter(e)
	}), true, false)
}

// Default balancer used by default
func Default() Balancer {
	return RandomChoice()
}
