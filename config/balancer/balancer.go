package balancer

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
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

func PreferLocal(b iface.Balancer) iface.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			b,
			func(cc conn.Conn, info info.Info) bool {
				return info.Local
			},
		),
	)
}

func PreferEndpoint(b iface.Balancer, preferredEndpoint string) iface.Balancer {
	return multi.Balancer(
		multi.WithBalancer(
			b,
			func(cc conn.Conn, info info.Info) bool {
				return info.Address == preferredEndpoint
			},
		),
	)
}

func Default() iface.Balancer {
	return PreferLocal(RandomChoice())
}
