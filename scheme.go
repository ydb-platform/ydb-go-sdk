package ydb_go_sdk_private

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type schemeWrapper struct {
	client *scheme.Client
}

func (s *schemeWrapper) set(cluster conn.Cluster, o options) {
	s.client = scheme.NewClient(cluster)
}
