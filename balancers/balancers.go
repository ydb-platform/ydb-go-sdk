package balancers

import (
	"strings"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Deprecated: RoundRobin is RandomChoice now
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

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// PreferLocalDC balancer try to autodetect local DC from client side.
func PreferLocalDC(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer.IsPreferConn = func(info balancerConfig.Info, c conn.Conn) bool {
		return c.Endpoint().Location() == info.SelfLocation
	}
	balancer.DetectlocalDC = true
	return balancer
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer *balancerConfig.Config) *balancerConfig.Config {
	balancer = PreferLocalDC(balancer)
	balancer.AllowFalback = true
	return balancer
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "ABC", "DEF", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	balancer.IsPreferConn = func(_ balancerConfig.Info, c conn.Conn) bool {
		location := strings.ToUpper(c.Endpoint().Location())
		for _, l := range locations {
			if location == l {
				return true
			}
		}
		return false
	}
	return balancer
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer *balancerConfig.Config, locations ...string) *balancerConfig.Config {
	balancer = PreferLocations(balancer, locations...)
	balancer.AllowFalback = true
	return balancer
}

type Endpoint interface {
	NodeID() uint32
	Address() string
	Location() string

	// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
	// It work good only if connection url always point to local dc.
	LocalDC() bool
}

// Prefer creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
func Prefer(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer.IsPreferConn = func(_ balancerConfig.Info, c conn.Conn) bool {
		return filter(c.Endpoint())
	}
	return balancer
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer *balancerConfig.Config, filter func(endpoint Endpoint) bool) *balancerConfig.Config {
	balancer = Prefer(balancer, filter)
	balancer.AllowFalback = true
	return balancer
}

// Default balancer used by default
func Default() *balancerConfig.Config {
	return RandomChoice()
}
