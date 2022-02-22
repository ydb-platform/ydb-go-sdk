package ydb

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/proxy"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type proxyConnection struct {
	connection Connection

	meta meta.Meta

	table        table.Client
	scripting    scripting.Client
	scheme       scheme.Client
	discovery    discovery.Client
	coordination coordination.Client
	ratelimiter  ratelimiter.Client
}

func (c *proxyConnection) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) (err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return err
	}
	return c.connection.Invoke(
		ctx,
		method,
		args,
		reply,
		opts...,
	)
}

func (c *proxyConnection) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (_ grpc.ClientStream, err error) {
	ctx, err = c.meta.Meta(ctx)
	if err != nil {
		return nil, err
	}
	return c.connection.NewStream(
		ctx,
		desc,
		method,
		opts...,
	)
}

func newProxy(connection Connection, meta meta.Meta) *proxyConnection {
	return &proxyConnection{
		connection: connection,
		meta:       meta,

		table:        proxy.Table(connection.Table(), meta),
		scripting:    proxy.Scripting(connection.Scripting(), meta),
		scheme:       proxy.Scheme(connection.Scheme(), meta),
		discovery:    proxy.Discovery(connection.Discovery(), meta),
		coordination: proxy.Coordination(connection.Coordination(), meta),
		ratelimiter:  proxy.Ratelimiter(connection.Ratelimiter(), meta),
	}
}

func (c *proxyConnection) Close(ctx context.Context) error {
	var issues []error
	if err := c.ratelimiter.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.coordination.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.scheme.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.table.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if err := c.scripting.Close(ctx); err != nil {
		issues = append(issues, err)
	}
	if len(issues) > 0 {
		return errors.NewWithIssues("close failed", issues...)
	}
	return nil
}

func (c *proxyConnection) Endpoint() string {
	return c.connection.Endpoint()
}

func (c *proxyConnection) Name() string {
	return c.meta.Database()
}

func (c *proxyConnection) Secure() bool {
	return c.connection.Secure()
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

func (c *proxyConnection) With(opts ...CustomOption) Connection {
	return newProxy(c, c.meta)
}
