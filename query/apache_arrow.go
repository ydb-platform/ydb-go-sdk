package query

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
)

// ArrowExecutor is an interface for execute queries with results in `Apache Arrow` format.
type ArrowExecutor interface {
}

type ArrowResult = arrow.Result
