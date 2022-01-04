package balancer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/rr"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/single"
)

func RoundRobin() iface.Balancer {
	return rr.RoundRobin()
}

func RandomChoice() iface.Balancer {
	return rr.RandomChoice()
}

func SingleConn() iface.Balancer {
	return single.Balancer()
}

func PreferLocal(primary iface.Balancer) iface.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool { return cc.Endpoint().LocalDC() }),
	)
}

func PreferLocalWithFallback(primary, fallback iface.Balancer) iface.Balancer {
	return multi.Balancer(
		multi.WithBalancer(primary, func(cc conn.Conn) bool { return cc.Endpoint().LocalDC() }),
		multi.WithBalancer(fallback, func(cc conn.Conn) bool { return !cc.Endpoint().LocalDC() }),
	)
}

func PreferEndpoints(primary iface.Balancer, endpoints ...string) iface.Balancer {
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

func PreferEndpointsWithFallback(primary, fallback iface.Balancer, endpoints ...string) iface.Balancer {
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

func Default() iface.Balancer {
	return PreferLocalWithFallback(RandomChoice(), RandomChoice())
}
