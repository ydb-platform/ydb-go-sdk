package connect

import (
	"context"
	"github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/scheme"
)

type schemeWrapper struct {
	ctx    context.Context
	client *scheme.Client
}

func newSchemeWrapper(ctx context.Context) *schemeWrapper {
	return &schemeWrapper{
		ctx: ctx,
	}
}

func (s *schemeWrapper) set(cluster ydb.Cluster) {
	s.client = scheme.NewClient(cluster)
}
