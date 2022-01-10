package balancer

import (
	"regexp"
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

func PreferLocal(primary ibalancer.Balancer) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool { return cc.Endpoint().LocalDC() }),
	)
}

func PreferLocalWithFallback(primary, fallback ibalancer.Balancer) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool { return cc.Endpoint().LocalDC() }),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool { return !cc.Endpoint().LocalDC() }),
	)
}

func PreferEndpoint(primary ibalancer.Balancer, endpoints ...string) ibalancer.Balancer {
	if len(endpoints) == 0 {
		panic("empty list of endpoints")
	}
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			address := cc.Endpoint().Address()
			for _, e := range endpoints {
				if address == e {
					return true
				}
			}
			return false
		}),
	)
}

func PreferEndpointRegEx(primary ibalancer.Balancer, re regexp.Regexp) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			return re.MatchString(cc.Endpoint().Address())
		}),
	)
}

func PreferEndpointWithFallback(primary, fallback ibalancer.Balancer, endpoints ...string) ibalancer.Balancer {
	if len(endpoints) == 0 {
		panic("empty list of endpoints")
	}
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			address := cc.Endpoint().Address()
			for _, e := range endpoints {
				if address == e {
					return true
				}
			}
			return false
		}),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool {
			address := cc.Endpoint().Address()
			for _, e := range endpoints {
				if address == e {
					return false
				}
			}
			return true
		}),
	)
}

func PreferEndpointWithFallbackRegEx(primary, fallback ibalancer.Balancer, re regexp.Regexp) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			return re.MatchString(cc.Endpoint().Address())
		}),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool {
			return !re.MatchString(cc.Endpoint().Address())
		}),
	)
}

func PreferLocations(primary ibalancer.Balancer, locations ...string) ibalancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
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

func PreferLocationsRegEx(primary ibalancer.Balancer, re regexp.Regexp) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			return re.MatchString(strings.ToUpper(cc.Endpoint().Location()))
		}),
	)
}

func PreferLocationsWithFallback(primary, fallback ibalancer.Balancer, locations ...string) ibalancer.Balancer {
	if len(locations) == 0 {
		panic("empty list of locations")
	}
	for i := range locations {
		locations[i] = strings.ToUpper(locations[i])
	}
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			location := strings.ToUpper(cc.Endpoint().Location())
			for _, l := range locations {
				if location == l {
					return true
				}
			}
			return false
		}),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool {
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

func PreferLocationsWithFallbackRegEx(primary, fallback ibalancer.Balancer, re regexp.Regexp) ibalancer.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool {
			return re.MatchString(strings.ToUpper(cc.Endpoint().Location()))
		}),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool {
			return !re.MatchString(strings.ToUpper(cc.Endpoint().Location()))
		}),
	)
}

func Default() ibalancer.Balancer {
	return PreferLocalWithFallback(RandomChoice(), RandomChoice())
}
