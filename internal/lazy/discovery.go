package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type lazyDiscovery struct {
	db     db.Connection
	trace  trace.Driver
	client discovery.Client
	m      sync.Mutex
}

func Discovery(db db.Connection, trace trace.Driver) *lazyDiscovery {
	return &lazyDiscovery{
		db:     db,
		trace:  trace,
		client: nil,
		m:      sync.Mutex{},
	}
}

func (d *lazyDiscovery) Discover(ctx context.Context) ([]endpoint.Endpoint, error) {
	d.init()
	return d.client.Discover(ctx)
}

func (d *lazyDiscovery) WhoAmI(ctx context.Context) (*discovery.WhoAmI, error) {
	d.init()
	return d.client.WhoAmI(ctx)
}

func (d *lazyDiscovery) Close(ctx context.Context) error {
	d.m.Lock()
	defer d.m.Unlock()
	if d.client == nil {
		return nil
	}
	defer func() {
		d.client = nil
	}()
	return d.client.Close(ctx)
}

func (d *lazyDiscovery) init() {
	d.m.Lock()
	if d.client == nil {
		d.client = discovery.New(d.db, d.db.Endpoint(), d.db.Name(), d.db.Secure(), d.trace)
	}
	d.m.Unlock()
}
