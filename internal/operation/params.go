package operation

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
)

func Params(
	timeout time.Duration,
	cancelAfter time.Duration,
	mode Mode,
) *Ydb_Operations.OperationParams {
	if timeout == 0 && cancelAfter == 0 && mode == 0 {
		return nil
	}
	return &Ydb_Operations.OperationParams{
		OperationMode:    mode.toYDB(),
		OperationTimeout: timeoutParam(timeout),
		CancelAfter:      timeoutParam(cancelAfter),
	}
}
