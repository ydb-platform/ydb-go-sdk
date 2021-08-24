package connect

import (
	"context"
	"github.com/YandexDatabase/ydb-go-sdk/v2"
	"github.com/YandexDatabase/ydb-go-sdk/v2/table"
)

type tableWrapper struct {
	ctx         context.Context
	client      *table.Client
	sessionPool *table.SessionPool
}

func newTableWrapper(ctx context.Context) *tableWrapper {
	return &tableWrapper{
		ctx: ctx,
		sessionPool: &table.SessionPool{
			Trace: table.ContextSessionPoolTrace(ctx),
		},
	}
}

func (t *tableWrapper) set(cluster ydb.Cluster) {
	t.client = table.NewClient(cluster, table.WithClientTraceOption(table.ContextClientTrace(t.ctx)))
	t.sessionPool.Builder = t.client
}

func (t *tableWrapper) CreateSession(ctx context.Context) (*table.Session, error) {
	return t.client.CreateSession(ctx)
}

func (t *tableWrapper) Pool() *table.SessionPool {
	return t.sessionPool
}
