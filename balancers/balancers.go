package balancers

import (
	"slices"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/strategy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Deprecated: RoundRobin is an alias to RandomChoice now
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func RoundRobin() strategy.Estimator {
	return RandomChoice()
}

func RandomChoice() strategy.Estimator {
	return strategy.RandomChoice()
}

func SingleConn() strategy.Estimator {
	return strategy.SingleConn()
}

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(estimator strategy.Estimator) strategy.Estimator {
	return PreferNearestDC(estimator)
}

// PreferNearestDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferNearestDC balancer try to autodetect local DC from client side.
func PreferNearestDC(estimator strategy.Estimator) strategy.Estimator {
	return strategy.PreferNearestDC(estimator, "LocalDC", func(info strategy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	}, false)
}

// Deprecated: use PreferNearestDCWithFallBack instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(estimator strategy.Estimator) strategy.Estimator {
	return PreferNearestDCWithFallBack(estimator)
}

// PreferNearestDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferNearestDCWithFallBack(estimator strategy.Estimator) strategy.Estimator {
	return strategy.PreferNearestDC(estimator, "LocalDC", func(info strategy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	}, true)
}

func locationsString(locations []string) string {
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
func PreferLocations(estimator strategy.Estimator, locations ...string) strategy.Estimator {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	// Prevent modify source locations
	locations = slices.Clone(locations)

	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return strategy.Prefer(estimator, locationsString(locations), func(_ strategy.Info, candidate endpoint.Info) bool {
		return slices.Contains(locations, strings.ToUpper(candidate.Location()))
	}, false)
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(estimator strategy.Estimator, locations ...string) strategy.Estimator {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	locations = slices.Clone(locations)
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return strategy.Prefer(estimator, locationsString(locations), func(_ strategy.Info, candidate endpoint.Info) bool {
		return slices.Contains(locations, strings.ToUpper(candidate.Location()))
	}, true)
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

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(child strategy.Estimator, filter func(endpoint Endpoint) bool) strategy.Estimator {
	return strategy.Prefer(child, "Custom", func(_ strategy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	}, false)
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(child strategy.Estimator, filter func(endpoint Endpoint) bool) strategy.Estimator {
	return strategy.Prefer(child, "Custom", func(_ strategy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	}, true)
}

// Default balancer used by default
func Default() strategy.Estimator {
	return RandomChoice()
}
