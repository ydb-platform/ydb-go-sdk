package query

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
)

// ArrowExecutor is an interface for execute queries with results in `Apache Arrow` format.
type ArrowExecutor interface {
	// QueryArrow like [Executor.Query] but returns results in [Apache Arrow] format.
	// Each part of the result implements io.Reader and contains the data in Arrow IPC format.
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	//
	// [Apache Arrow]: https://arrow.apache.org/
	QueryArrow(ctx context.Context, sql string, opts ...ExecuteOption) (ArrowResult, error)
}

type ArrowResult = arrow.Result
