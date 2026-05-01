package balancers

import (
	"slices"
	"sort"
	"strings"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xstring"
)

// IPType is a bit-mask that controls which IP address families are permitted
// when the gRPC resolver forwards resolved addresses to the connection manager.
//
// The filter is applied to all resolved addresses, whether they came from DNS
// resolution of a FQDN or were literal IP addresses in the discovery response.
//
// The constants can be combined with the bitwise OR operator:
//
//	balancers.WithTypeIP(balancers.IPv4 | balancers.IPv6) // both families (default)
//	balancers.WithTypeIP(balancers.IPv6)                  // IPv6 only
//	balancers.WithTypeIP(balancers.IPv4)                  // IPv4 only
type IPType = balancerConfig.IPType

const (
	// IPv4 allows IPv4 resolved addresses when dialing cluster endpoints.
	IPv4 = balancerConfig.IPv4
	// IPv6 allows IPv6 resolved addresses when dialing cluster endpoints.
	IPv6 = balancerConfig.IPv6
	// AllIPTypes allows both IPv4 and IPv6 addresses (the default).
	AllIPTypes = balancerConfig.AllIPTypes
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

// WithTypeIP configures the SDK to use only IP addresses of the allowed families
// when establishing connections to cluster endpoints.
//
// The filter is applied at the gRPC resolver level: after DNS resolution of a
// cluster endpoint FQDN, only the resolved addresses whose IP family matches
// the mask are passed to the gRPC connection balancer. All matching addresses
// are kept, preserving the round-robin connection behavior.
//
// Example – IPv6-only (useful in environments where outbound IPv4 is blocked):
//
//	ydb.Open(ctx, dsn, ydb.WithBalancer(balancers.WithTypeIP(balancers.IPv6)))
//
// The returned *balancerConfig.Config uses RandomChoice as its base balancing
// algorithm. Wrap it with other balancer functions if a different algorithm
// is needed:
//
//	ydb.WithBalancer(balancers.PreferNearestDC(balancers.WithTypeIP(balancers.IPv6)))
func WithTypeIP(t IPType) *balancerConfig.Config {
	c := RandomChoice()
	c.AllowedIPTypes = t

	return c
}

// Default balancer used by default
func Default() *balancerConfig.Config {
	return RandomChoice()
}
