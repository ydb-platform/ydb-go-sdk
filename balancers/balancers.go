package balancers

import (
	"slices"
	"sort"
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/policy"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// Deprecated: RoundRobin is an alias to RandomChoice now
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func RoundRobin() policy.Policy {
	return RandomChoice()
}

func RandomChoice() policy.Policy {
	return policy.Policy{}
}

func SingleConn() policy.Policy {
	return policy.SingleConn()
}

// WithMaxConnections limits the number of discovered endpoints kept in the
// balancer's active connection set. The limit is sticky across discovery
// updates: healthy active endpoints are retained when possible.
//
// [WithNodeID] may create an additional connection outside the active set to
// preserve node-affinity contracts, so the limit is intentionally soft.
// Zero disables the limit. Negative values are treated as zero.
func WithMaxConnections(p policy.Policy, maxConnections int) policy.Policy {
	return policy.WithMaxConnections(p, maxConnections)
}

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(p policy.Policy) policy.Policy {
	return PreferNearestDC(p)
}

// PreferNearestDC uses only endpoints in the location nearest to the client.
func PreferNearestDC(p policy.Policy) policy.Policy {
	return policy.PreferNearestDC(p, "LocalDC", func(info policy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	})
}

// Deprecated: use PreferNearestDCWithFallBack instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(p policy.Policy) policy.Policy {
	return PreferNearestDCWithFallBack(p)
}

// PreferNearestDCWithFallBack prioritizes endpoints in the location nearest to the client.
// Endpoints in other locations are used when all nearer endpoints are unavailable.
func PreferNearestDCWithFallBack(p policy.Policy) policy.Policy {
	return policy.PreferNearestDCWithFallback(p, "LocalDC", func(info policy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	})
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

// PreferLocations uses only endpoints in the selected locations (such as "ABC", "DEF", etc.).
func PreferLocations(p policy.Policy, locations ...string) policy.Policy {
	return preferLocations(p, false, locations)
}

func preferLocations(p policy.Policy, allowFallback bool, locations []string) policy.Policy {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	locations = slices.Clone(locations)
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	match := func(_ policy.Info, candidate endpoint.Info) bool {
		return slices.Contains(locations, strings.ToUpper(candidate.Location()))
	}
	if allowFallback {
		return policy.PreferWithFallback(p, locationsString(locations), match)
	}

	return policy.Prefer(p, locationsString(locations), match)
}

// PreferLocationsWithFallback prioritizes endpoints in the selected locations.
// Endpoints in other locations are used when all preferred endpoints are unavailable.
func PreferLocationsWithFallback(p policy.Policy, locations ...string) policy.Policy {
	return preferLocations(p, true, locations)
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

// Prefer uses only endpoints accepted by filter.
func Prefer(p policy.Policy, filter func(endpoint Endpoint) bool) policy.Policy {
	return policy.Prefer(p, "Custom", func(_ policy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	})
}

// PreferWithFallback prioritizes endpoints accepted by filter.
// Other endpoints are used when all preferred endpoints are unavailable.
func PreferWithFallback(p policy.Policy, filter func(endpoint Endpoint) bool) policy.Policy {
	return policy.PreferWithFallback(p, "Custom", func(_ policy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	})
}

// Default balancer used by default
func Default() policy.Policy {
	return RandomChoice()
}
