package lazy

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/db"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type lazyDiscovery struct {
	db     db.Connection
	trace  trace.Driver
	client discovery.Client
	m      sync.Mutex
}

func Discovery(db db.Connection, trace trace.Driver) discovery.Client {
	return &lazyDiscovery{
		db:     db,
		trace:  trace,
		client: nil,
		m:      sync.Mutex{},
	}
}

func (d *lazyDiscovery) Discover(ctx context.Context) (endpoints []endpoint.Endpoint, err error) {
	d.init()
	err = retry.Retry(ctx, true, func(ctx context.Context) (err error) {
		endpoints, err = d.client.Discover(ctx)
		return err
	})
	return endpoints, err
}

func (d *lazyDiscovery) WhoAmI(ctx context.Context) (whoAmI *discovery.WhoAmI, err error) {
	d.init()
	err = retry.Retry(ctx, true, func(ctx context.Context) (err error) {
		whoAmI, err = d.client.WhoAmI(ctx)
		return err
	})
	return whoAmI, err
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
		d.client = builder.New(d.db, d.db.Endpoint(), d.db.Name(), d.db.Secure(), d.trace)
	}
	d.m.Unlock()
}
