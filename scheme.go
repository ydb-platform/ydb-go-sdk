package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/scheme"
)

type lazyScheme struct {
	client *scheme.Client
}

func (s *lazyScheme) set(cluster cluster.Cluster, o options) {
	s.client = scheme.NewClient(cluster)
}
