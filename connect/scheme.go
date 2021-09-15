package connect

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type schemeWrapper struct {
	client *scheme.Client
}

func (s *schemeWrapper) set(cluster conn.Cluster, o options) {
	s.client = scheme.NewClient(cluster)
}
