package xsql

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

type (
	ctxTransactionControlKey struct{}
	ctxDataQueryOptionsKey   struct{}
	ctxScanQueryOptionsKey   struct{}
	ctxModeTypeKey           struct{}
)

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

func WithScanQueryOptions(ctx context.Context, opts []options.ExecuteScanQueryOption) context.Context {
	return context.WithValue(ctx, ctxScanQueryOptionsKey{}, append(scanQueryOptions(ctx), opts...))
}

func scanQueryOptions(ctx context.Context) []options.ExecuteScanQueryOption {
	if opts, ok := ctx.Value(ctxScanQueryOptionsKey{}).([]options.ExecuteScanQueryOption); ok {
		return opts
	}
	return nil
}

func WithDataQueryOptions(ctx context.Context, opts []options.ExecuteDataQueryOption) context.Context {
	return context.WithValue(ctx, ctxDataQueryOptionsKey{}, append(dataQueryOptions(ctx), opts...))
}

func dataQueryOptions(ctx context.Context) []options.ExecuteDataQueryOption {
	if opts, ok := ctx.Value(ctxDataQueryOptionsKey{}).([]options.ExecuteDataQueryOption); ok {
		return opts
	}
	return nil
}
