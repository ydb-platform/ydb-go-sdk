package ydb

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	internalStats "github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

// WithOperationTimeout returns a copy of parent context in which YDB operation timeout
// parameter is set to d. If parent context timeout is smaller than d, parent context is returned.
func WithOperationTimeout(ctx context.Context, operationTimeout time.Duration) context.Context {
	return operation.WithTimeout(ctx, operationTimeout)
}

// WithOperationCancelAfter returns a copy of parent context in which YDB operation
// cancel after parameter is set to d. If parent context cancellation timeout is smaller
// than d, parent context is returned.
func WithOperationCancelAfter(ctx context.Context, operationCancelAfter time.Duration) context.Context {
	return operation.WithCancelAfter(ctx, operationCancelAfter)
}

// WithPreferredNodeID allows to set preferred node to get session from
func WithPreferredNodeID(ctx context.Context, nodeID uint32) context.Context {
	return endpoint.WithNodeID(ctx, nodeID)
}

// WithStatsModeBasic sets basic stats collection mode for database/sql queries.
// The callback will be called with query execution statistics after query execution.
func WithStatsModeBasic(ctx context.Context, callback func(stats.QueryStats)) context.Context {
	return internalStats.WithModeCallback(ctx, internalStats.ModeBasic, callback)
}

// WithStatsModeFull sets full stats collection mode for database/sql queries.
// The callback will be called with query execution statistics after query execution.
func WithStatsModeFull(ctx context.Context, callback func(stats.QueryStats)) context.Context {
	return internalStats.WithModeCallback(ctx, internalStats.ModeFull, callback)
}

// WithStatsModeProfile sets profile stats collection mode for database/sql queries.
// The callback will be called with query execution statistics after query execution.
func WithStatsModeProfile(ctx context.Context, callback func(stats.QueryStats)) context.Context {
	return internalStats.WithModeCallback(ctx, internalStats.ModeProfile, callback)
}
