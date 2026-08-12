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

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(p policy.Policy) policy.Policy {
	return PreferNearestDC(p)
}

// PreferNearestDC prioritizes endpoints in the location nearest to the client.
// Endpoints in other locations are used when all nearer endpoints are unavailable.
func PreferNearestDC(p policy.Policy) policy.Policy {
	return policy.PreferNearestDC(p, "LocalDC", func(info policy.Info, candidate endpoint.Info) bool {
		return candidate.Location() == info.SelfLocation
	})
}

// Deprecated: use PreferNearestDCWithFallBack instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(p policy.Policy) policy.Policy {
	return PreferNearestDC(p)
}

// PreferNearestDCWithFallBack is an alias for PreferNearestDC.
func PreferNearestDCWithFallBack(p policy.Policy) policy.Policy {
	return PreferNearestDC(p)
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
func PreferLocations(p policy.Policy, locations ...string) policy.Policy {
	return preferLocations(p, locations)
}

func preferLocations(p policy.Policy, locations []string) policy.Policy {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	locations = slices.Clone(locations)
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)

	return policy.Prefer(p, locationsString(locations), func(_ policy.Info, candidate endpoint.Info) bool {
		return slices.Contains(locations, strings.ToUpper(candidate.Location()))
	})
}

// PreferLocationsWithFallback is an alias for PreferLocations.
func PreferLocationsWithFallback(p policy.Policy, locations ...string) policy.Policy {
	return PreferLocations(p, locations...)
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
func Prefer(p policy.Policy, filter func(endpoint Endpoint) bool) policy.Policy {
	return policy.Prefer(p, "Custom", func(_ policy.Info, candidate endpoint.Info) bool {
		return filter(candidate)
	})
}

// PreferWithFallback is an alias for Prefer.
func PreferWithFallback(p policy.Policy, filter func(endpoint Endpoint) bool) policy.Policy {
	return Prefer(p, filter)
}

// Default balancer used by default
func Default() policy.Policy {
	return RandomChoice()
}
