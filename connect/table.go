package connect

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"context"
	"sync"
)

type tableWrapper struct {
	ctx             context.Context
	client          *table.Client
	sessionPoolOnce sync.Once
	sessionPool     *table.SessionPool
}

func newTableWrapper(ctx context.Context, driver ydb.Driver) *tableWrapper {
	return &tableWrapper{
		ctx: ctx,
		client: &table.Client{
			Driver: driver,
			Trace:  table.ContextClientTrace(ctx),
		},
	}
}

func (t *tableWrapper) CreateSession(ctx context.Context) (*table.Session, error) {
	return t.client.CreateSession(ctx)
}

func (t *tableWrapper) Pool() *table.SessionPool {
	t.sessionPoolOnce.Do(func() {
		t.sessionPool = &table.SessionPool{
			Builder: t.client,
			Trace:   table.ContextSessionPoolTrace(t.ctx),
		}
	})
	return t.sessionPool
}
