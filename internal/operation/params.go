package operation

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
)

func Params(
	ctx context.Context,
	timeout time.Duration,
	cancelAfter time.Duration,
	mode Mode,
) *Ydb_Operations.OperationParams {
	b := &Ydb_Operations.OperationParams{}

	// Set operation timeout from context or parameter
	if d, ok := ctxTimeout(ctx); ok {
		b.OperationTimeout = timeoutParam(d)
	} else if timeout > 0 {
		b.OperationTimeout = timeoutParam(timeout)
	}

	// Override with context deadline if available and less than timeout
	if d, ok := ctxUntilDeadline(ctx); mode == ModeSync && ok && d < timeout {
		b.OperationTimeout = timeoutParam(d)
	}

	// Set cancel after from context or parameter
	if d, ok := ctxCancelAfter(ctx); ok {
		b.CancelAfter = timeoutParam(d)
	} else if cancelAfter > 0 {
		b.CancelAfter = timeoutParam(cancelAfter)
	}

	// Set operation mode
	b.OperationMode = mode.toYDB()

	// If no fields are set, return nil
	if b.OperationTimeout == nil && b.CancelAfter == nil && b.OperationMode == Ydb_Operations.OperationParams_OPERATION_MODE_UNSPECIFIED {
		return nil
	}

	return b
}
