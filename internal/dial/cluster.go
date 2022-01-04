package dial

import (
	ibalancer "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
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
