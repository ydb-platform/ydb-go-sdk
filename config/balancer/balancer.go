package balancer

import (
	"regexp"

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

func Default() ibalancer.Balancer {
	return PreferLocalWithFallback(RandomChoice(), RandomChoice())
}
