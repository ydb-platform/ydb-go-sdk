package dial

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func (d *dialer) newCluster(trace trace.Driver) cluster.Cluster {
	return cluster.New(
		trace,
		d.dial,
		func() balancer.Balancer {
			if d.config.DiscoveryInterval() == 0 {
				return balancer.Single()
			}
			return balancer.New(d.config.BalancingConfig())
		}(),
	)
}
