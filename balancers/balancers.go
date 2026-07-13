package balancers

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// IPVersion identifies a requested IP address version.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type IPVersion = balancerConfig.IPVersion

const (
	// IPv6 selects IPv6 addresses.
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	IPv6 = balancerConfig.IPv6
)

// Deprecated: RoundRobin is an alias to RandomChoice now
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func RoundRobin() *balancerConfig.Config {
	return &balancerConfig.Config{}
}

func RandomChoice() *balancerConfig.Config {
	return &balancerConfig.Config{}
}

func SingleConn() *balancerConfig.Config {
	return &balancerConfig.Config{
		SingleConn: true,
	}
}

type filterLocalDC struct{}

func (filterLocalDC) Allow(info balancerConfig.Info, e endpoint.Info) bool {
	return e.Location() == info.SelfLocation
}

func (filterLocalDC) String() string {
	return "LocalDC"
}

// Deprecated: use PreferNearestDC instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDC(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer.Filter = filterLocalDC{}
	balancer.DetectNearestDC = true

	return balancer
}

// PreferNearestDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferNearestDC balancer try to autodetect local DC from client side.
func PreferNearestDC(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer.Filter = filterLocalDC{}
	balancer.DetectNearestDC = true

	return balancer
}

// Deprecated: use PreferNearestDCWithFallBack instead
// Will be removed after March 2025.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func PreferLocalDCWithFallBack(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer = PreferNearestDC(balancer)
	balancer.AllowFallback = true

	return balancer
}

// PreferNearestDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferNearestDCWithFallBack(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer = PreferNearestDC(balancer)
	balancer.AllowFallback = true

	return balancer
}

type filterLocations []string

func (locations filterLocations) Allow(_ balancerConfig.Info, e endpoint.Info) bool {
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
func PreferLocations(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	if len(locations) == 0 {
		panic("empty list of locations")
	}

	// Prevent modify source locations
	locations = slices.Clone(locations)

	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	sort.Strings(locations)
	balancer.Filter = filterLocations(locations)

	return balancer
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	balancer = PreferLocations(balancer, locations...)
	balancer.AllowFallback = true

	return balancer
}

// OnlyIPVersion creates a balancer that establishes connections only to addresses of the specified IP version.
//
// IPv6 is the only supported IP version. FQDNs received from discovery are resolved by gRPC and all IPv6
// addresses are retained. IPv4 addresses are not passed to gRPC for connection attempts.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func OnlyIPVersion(balancer *balancerConfig.Config, version IPVersion) *balancerConfig.Config {
	switch version {
	case IPv6:
		balancer.IPVersion = version
	default:
		panic(fmt.Sprintf("unsupported IP version: %d", version))
	}

	return balancer
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

type filterFunc func(info balancerConfig.Info, e endpoint.Info) bool

func (p filterFunc) Allow(info balancerConfig.Info, e endpoint.Info) bool {
	return p(info, e)
}

func (p filterFunc) String() string {
	return "Custom"
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer.Filter = filterFunc(func(_ balancerConfig.Info, e endpoint.Info) bool {
		return filter(e)
	})

	return balancer
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer = Prefer(balancer, filter)
	balancer.AllowFallback = true

	return balancer
}

// Default balancer used by default
func Default() *balancerConfig.Config {
	return RandomChoice()
}
