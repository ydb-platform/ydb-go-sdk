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
func RoundRobin() strategy.Policy {
	return RandomChoice()
}

func RandomChoice() strategy.Policy {
	return strategy.Policy{}
}

func SingleConn() strategy.Policy {
	return strategy.SingleConn()
}

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(policy strategy.Policy) strategy.Policy {
	return PreferNearestDC(policy)
}

// PreferNearestDC prioritizes endpoints in the location nearest to the client.
// Endpoints in other locations are used when all nearer endpoints are unavailable.
func PreferNearestDC(policy strategy.Policy) strategy.Policy {
	return strategy.PreferNearestDC(policy, "LocalDC", func(info strategy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	})
}

// Deprecated: use PreferNearestDC instead.
// All preference policies automatically cascade to lower-priority endpoints.
// Will be removed after March 2027.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(policy strategy.Policy) strategy.Policy {
	return PreferNearestDC(policy)
}

// Deprecated: use PreferNearestDC instead.
// All preference policies automatically cascade to lower-priority endpoints.
// Will be removed after March 2027.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferNearestDCWithFallBack(policy strategy.Policy) strategy.Policy {
	return PreferNearestDC(policy)
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

// PreferLocations prioritizes endpoints in the selected locations (such as "ABC", "DEF", etc.).
// Endpoints in other locations are used when all preferred endpoints are unavailable.
func PreferLocations(policy strategy.Policy, locations ...string) strategy.Policy {
	return preferLocations(policy, locations)
}

func preferLocations(policy strategy.Policy, locations []string) strategy.Policy {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	locations = slices.Clone(locations)
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return strategy.Prefer(policy, locationsString(locations), func(_ strategy.Info, candidate endpoint.Info) bool {
		return slices.Contains(locations, strings.ToUpper(candidate.Location()))
	})
}

// Deprecated: use PreferLocations instead.
// All preference policies automatically cascade to lower-priority endpoints.
// Will be removed after March 2027.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocationsWithFallback(policy strategy.Policy, locations ...string) strategy.Policy {
	return PreferLocations(policy, locations...)
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

// Prefer prioritizes endpoints accepted by filter.
// Other endpoints are used when all preferred endpoints are unavailable.
func Prefer(policy strategy.Policy, filter func(endpoint Endpoint) bool) strategy.Policy {
	return strategy.Prefer(policy, "Custom", func(_ strategy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	})
}

// Deprecated: use Prefer instead.
// All preference policies automatically cascade to lower-priority endpoints.
// Will be removed after March 2027.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferWithFallback(policy strategy.Policy, filter func(endpoint Endpoint) bool) strategy.Policy {
	return Prefer(policy, filter)
}

// Default balancer used by default
func Default() strategy.Policy {
	return RandomChoice()
}
