package ydb

import (
	"context"
)

// Driver is an interface of YDB driver.
// Deprecated: use Cluster interface instead
type Driver interface {
	Call(context.Context, Operation) (CallInfo, error)
	StreamRead(context.Context, StreamOperation) (CallInfo, error)
	Close() error
}
