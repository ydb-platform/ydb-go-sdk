package dial

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	ibalancer "github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/iface"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func (d *dialer) newCluster(trace trace.Driver) cluster.Cluster {
	return cluster.New(
		trace,
		d.dial,
		func() ibalancer.Balancer {
			if d.config.DiscoveryInterval() == 0 {
				return single.Balancer()
			}
			return d.config.Balancer()
		}(),
	)
}
