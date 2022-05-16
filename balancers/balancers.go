package balancers

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func RoundRobin() balancer.Balancer {
	return &balancer.Config{}
}

func RandomChoice() balancer.Balancer {
	return &balancer.Config{}
}

func SingleConn() balancer.Balancer {
	return &balancer.Config{
		SingleConn: true,
	}
}

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocalDC(balancer balancer.Balancer) balancer.Balancer {
	balancer.IsPreferConn = func(c conn.Conn) bool {
		return c.Endpoint().LocalDC()
	}
	return balancer
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer balancer.Balancer) balancer.Balancer {
	balancer = PreferLocalDC(balancer)
	balancer.AllowFalback = true
	return balancer
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
	balancer.IsPreferConn = func(c conn.Conn) bool {
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
func PreferLocationsWithFallback(balancer balancer.Balancer, locations ...string) balancer.Balancer {
	balancer = PreferLocations(balancer, locations...)
	balancer.AllowFalback = true
	return balancer
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
	balancer.IsPreferConn = func(c conn.Conn) bool {
		return filter(c.Endpoint())
	}
	return balancer
}

// PreferWithFallback creates balancer which use endpoints by filter
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferWithFallback(balancer balancer.Balancer, filter func(endpoint Endpoint) bool) balancer.Balancer {
	balancer = Prefer(balancer, filter)
	balancer.AllowFalback = true
	return balancer
}

// Default balancer used by default
func Default() balancer.Balancer {
	return RandomChoice()
}
