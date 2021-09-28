package ydb

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"sync"
)

type lazyDiscovery struct {
	db     DB
	client discovery.Client
	m      sync.Mutex
}

func (t *lazyDiscovery) Discover(ctx context.Context) ([]endpoint.Endpoint, error) {
	t.init()
	return t.client.Discover(ctx)
}

func (t *lazyDiscovery) WhoAmI(ctx context.Context) (*discovery.WhoAmI, error) {
	t.init()
	return t.client.WhoAmI(ctx)
}

func (t *lazyDiscovery) Close(ctx context.Context) error {
	t.m.Lock()
	defer t.m.Unlock()
	if t.client == nil {
		return nil
	}
	defer func() {
		t.client = nil
	}()
	return t.client.Close(ctx)
}

func newDiscovery(db DB) *lazyDiscovery {
	return &lazyDiscovery{
		db: db,
	}
}

func (t *lazyDiscovery) init() {
	t.m.Lock()
	if assert.IsNil(t.client) {
		t.client = discovery.New(t.db, t.db.Name(), t.db.Secure())
	}
	t.m.Unlock()
}
