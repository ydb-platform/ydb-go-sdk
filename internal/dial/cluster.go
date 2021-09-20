package dial

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
)

func (d *dialer) newCluster() cluster.Cluster {
	return cluster.New(
		d.dialHostPort,
		func() balancer.Balancer {
			if d.config.DiscoveryInterval == 0 {
				return balancer.Single()
			}
			return balancer.New(d.config.BalancingConfig)
		}(),
	)
}
