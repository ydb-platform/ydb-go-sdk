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
	b := Ydb_Operations.OperationParams_builder{}
	if d, ok := ctxTimeout(ctx); ok {
		b.OperationTimeout = timeoutParam(d)
	}
	if d, ok := ctxUntilDeadline(ctx); mode == ModeSync && ok && d < timeout {
		b.OperationTimeout = timeoutParam(d)
	}
	if d := cancelAfter; d > 0 {
		b.CancelAfter = timeoutParam(d)
	}
	if d, ok := ctxCancelAfter(ctx); ok && d < cancelAfter {
		b.CancelAfter = timeoutParam(d)
	}

	return b.Build()
}
