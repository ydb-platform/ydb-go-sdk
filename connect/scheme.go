package connect

import (
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type schemeWrapper struct {
	client *scheme.Client
}

func (s *schemeWrapper) set(cluster ydb.Cluster, o options) {
	s.client = scheme.NewClient(cluster)
}
