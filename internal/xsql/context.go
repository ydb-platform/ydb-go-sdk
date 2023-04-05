package xsql

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/bind"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type (
	ctxTransactionControlKey struct{}
	ctxDataQueryOptionsKey   struct{}
	ctxScanQueryOptionsKey   struct{}
	ctxModeTypeKey           struct{}
)

type queryBindConnectorOption struct {
	bind bind.Bind
}

func (q queryBindConnectorOption) RewriteQuery(
	query string, args ...interface{},
) (yql string, newArgs []interface{}, _ error) {
	return q.bind.RewriteQuery(query, args...)
}

func (q queryBindConnectorOption) Apply(c *Connector) error {
	c.Bindings = append(c.Bindings, q.bind)
	return nil
}

type queryBindAndPathNormalizerConnectorOption struct {
	bindAndPathNormalizer queryBindAndPathNormalizer
}

func (q queryBindAndPathNormalizerConnectorOption) RewriteQuery(
	query string, args ...interface{},
) (yql string, newArgs []interface{}, _ error) {
	return q.bindAndPathNormalizer.RewriteQuery(query, args...)
}

func (q queryBindAndPathNormalizerConnectorOption) Apply(c *Connector) error {
	c.Bindings = append(c.Bindings, q.bindAndPathNormalizer)
	c.PathNormalizer = q.bindAndPathNormalizer
	return nil
}

func WithQueryBind(bind bind.Bind) QueryBindConnectorOption {
	return queryBindConnectorOption{bind: bind}
}

type queryBindAndPathNormalizer interface {
	bind.Bind
	pathNormalizer
}

func WithQueryBindAndPathNormalizer(bindAndPathNormalizer queryBindAndPathNormalizer) QueryBindConnectorOption {
	return queryBindAndPathNormalizerConnectorOption{
		bindAndPathNormalizer: bindAndPathNormalizer,
	}
}

// WithQueryMode returns a copy of context with given QueryMode
func WithQueryMode(ctx context.Context, m QueryMode) context.Context {
	return context.WithValue(ctx, ctxModeTypeKey{}, m)
}

// queryModeFromContext returns defined QueryMode or DefaultQueryMode
func queryModeFromContext(ctx context.Context, defaultQueryMode QueryMode) QueryMode {
	if m, ok := ctx.Value(ctxModeTypeKey{}).(QueryMode); ok {
		return m
	}
	return defaultQueryMode
}

func WithTxControl(ctx context.Context, txc *table.TransactionControl) context.Context {
	return context.WithValue(ctx, ctxTransactionControlKey{}, txc)
}

func txControl(ctx context.Context, defaultTxControl *table.TransactionControl) *table.TransactionControl {
	if txc, ok := ctx.Value(ctxTransactionControlKey{}).(*table.TransactionControl); ok {
		return txc
	}
	return defaultTxControl
}

func (c *conn) WithScanQueryOptions(ctx context.Context, opts ...options.ExecuteScanQueryOption) context.Context {
	return context.WithValue(ctx,
		ctxScanQueryOptionsKey{},
		append(
			append([]options.ExecuteScanQueryOption{}, c.scanQueryOptions(ctx)...),
			opts...,
		),
	)
}

func (c *conn) scanQueryOptions(ctx context.Context) []options.ExecuteScanQueryOption {
	if opts, ok := ctx.Value(ctxScanQueryOptionsKey{}).([]options.ExecuteScanQueryOption); ok {
		return append(c.scanOpts, opts...)
	}
	return c.scanOpts
}

func (c *conn) WithDataQueryOptions(ctx context.Context, opts ...options.ExecuteDataQueryOption) context.Context {
	return context.WithValue(ctx,
		ctxDataQueryOptionsKey{},
		append(
			append([]options.ExecuteDataQueryOption{}, c.dataQueryOptions(ctx)...),
			opts...,
		),
	)
}

func (c *conn) dataQueryOptions(ctx context.Context) []options.ExecuteDataQueryOption {
	if opts, ok := ctx.Value(ctxDataQueryOptionsKey{}).([]options.ExecuteDataQueryOption); ok {
		return append(c.dataOpts, opts...)
	}
	return c.dataOpts
}

func (c *conn) withKeepInCache(ctx context.Context) context.Context {
	return c.WithDataQueryOptions(ctx, options.WithKeepInCache(true))
}
