package ydb

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type lazyDiscovery struct {
	db     DB
	trace  trace.Driver
	client discovery.Client
	m      sync.Mutex
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
		d.client = discovery.New(d.db, d.db.Name(), d.db.Secure(), d.trace)
	}
	d.m.Unlock()
}
