package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/proxy"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type proxyConnection struct {
	endpoint string
	secure   bool
	meta     meta.Meta

	table        table.Client
	scripting    scripting.Client
	scheme       scheme.Client
	discovery    discovery.Client
	coordination coordination.Client
	ratelimiter  ratelimiter.Client
}

func newProxy(c Connection, meta meta.Meta) *proxyConnection {
	return &proxyConnection{
		endpoint: c.Endpoint(),
		secure:   c.Secure(),
		meta:     meta,

		table:        proxy.Table(c.Table(), meta),
		scripting:    proxy.Scripting(c.Scripting(), meta),
		scheme:       proxy.Scheme(c.Scheme(), meta),
		discovery:    proxy.Discovery(c.Discovery(), meta),
		coordination: proxy.Coordination(c.Coordination(), meta),
		ratelimiter:  proxy.Ratelimiter(c.Ratelimiter(), meta),
	}
}

func (c *proxyConnection) Close(ctx context.Context) error {
	// nop
	return nil
}

func (c *proxyConnection) Endpoint() string {
	return c.endpoint
}

func (c *proxyConnection) Name() string {
	return c.meta.Database()
}

func (c *proxyConnection) Secure() bool {
	return c.secure
}

func (c *proxyConnection) Table(opts ...CustomOption) table.Client {
	if len(opts) == 0 {
		return c.table
	}
	return proxy.Table(c.table, newMeta(c.meta, opts...))
}

func (c *proxyConnection) Scheme(opts ...CustomOption) scheme.Client {
	return proxy.Scheme(c.scheme, newMeta(c.meta, opts...))
}

func (c *proxyConnection) Coordination(opts ...CustomOption) coordination.Client {
	return proxy.Coordination(c.coordination, newMeta(c.meta, opts...))
}

func (c *proxyConnection) Ratelimiter(opts ...CustomOption) ratelimiter.Client {
	return proxy.Ratelimiter(c.ratelimiter, newMeta(c.meta, opts...))
}

func (c *proxyConnection) Discovery(opts ...CustomOption) discovery.Client {
	return proxy.Discovery(c.discovery, newMeta(c.meta, opts...))
}

func (c *proxyConnection) Scripting(opts ...CustomOption) scripting.Client {
	return proxy.Scripting(c.scripting, newMeta(c.meta, opts...))
}

func (c *proxyConnection) WithCustomOptions(opts ...CustomOption) Connection {
	return newProxy(c, c.meta)
}
