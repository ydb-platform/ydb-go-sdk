package balancer

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/rr"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func RoundRobin() ibalancer.Balancer {
	return rr.RoundRobin()
}

func RandomChoice() ibalancer.Balancer {
	return rr.RandomChoice()
}

func SingleConn() ibalancer.Balancer {
	return single.Balancer()
}

// PreferLocalDC creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocalDC(balancer ibalancer.Balancer) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			balancer,
			func(cc conn.Conn) bool {
				return cc.Endpoint().LocalDC()
			},
		),
	)
}

// PreferLocalDCWithFallBack creates balancer which use endpoints only in location such as initial endpoint location
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocalDCWithFallBack(balancer ibalancer.Balancer) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			balancer,
			func(cc conn.Conn) bool {
				return cc.Endpoint().LocalDC()
			},
		),
		multi.WithBalancer(
			balancer.(ibalancer.Creator).Create(),
			func(cc conn.Conn) bool {
				return !cc.Endpoint().LocalDC()
			},
		),
	)
}

// PreferLocations creates balancer which use endpoints only in selected locations (such as "MAN", "VLA", etc.)
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
func PreferLocations(balancer ibalancer.Balancer, locations ...string) ibalancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return multi.Balancer(
		multi.WithBalancer(balancer, func(cc conn.Conn) bool {
			location := strings.ToUpper(cc.Endpoint().Location())
			for _, l := range locations {
				if location == l {
					return true
				}
			}
			return false
		}),
	)
}

// PreferLocationsWithFallback creates balancer which use endpoints only in selected locations
// Balancer "balancer" defines balancing algorithm between endpoints selected with filter by location
// If filter returned zero endpoints from all discovery endpoints list - used all endpoint instead
func PreferLocationsWithFallback(balancer ibalancer.Balancer, locations ...string) ibalancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return multi.Balancer(
		multi.WithBalancer(balancer, func(cc conn.Conn) bool {
			location := strings.ToUpper(cc.Endpoint().Location())
			for _, l := range locations {
				if location == l {
					return true
				}
			}
			return false
		}),
		multi.WithBalancer(balancer.(ibalancer.Creator).Create(), func(cc conn.Conn) bool {
			location := strings.ToUpper(cc.Endpoint().Location())
			for _, l := range locations {
				if location == l {
					return false
				}
			}
			return true
		}),
	)
}

func Default() ibalancer.Balancer {
	return PreferLocalDCWithFallBack(RandomChoice())
}
