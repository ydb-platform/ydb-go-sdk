package proxy

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
)

type proxyDiscovery struct {
	client discovery.Client
	meta   meta.Meta
}

func Discovery(client discovery.Client, meta meta.Meta) discovery.Client {
	return &proxyDiscovery{
		client: client,
		meta:   meta,
	}
}

func (d *proxyDiscovery) Discover(ctx context.Context) (_ []endpoint.Endpoint, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.Discover(ctx)
}

func (d *proxyDiscovery) WhoAmI(ctx context.Context) (_ *discovery.WhoAmI, err error) {
	ctx, err = d.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return d.client.WhoAmI(ctx)
}

func (d *proxyDiscovery) Close(ctx context.Context) (err error) {
	// nop
	return nil
}
