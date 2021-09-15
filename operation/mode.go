package operation

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

type OperationMode uint

const (
	OperationModeUnknown OperationMode = iota
	OperationModeSync
	OperationModeAsync
)

func (m OperationMode) String() string {
	switch m {
	case OperationModeSync:
		return "sync"
	case OperationModeAsync:
		return "async"
	default:
		return "unknown"
	}
}
func (m OperationMode) toYDB() Ydb_Operations.OperationParams_OperationMode {
	switch m {
	case OperationModeSync:
		return Ydb_Operations.OperationParams_SYNC
	case OperationModeAsync:
		return Ydb_Operations.OperationParams_ASYNC
	default:
		return Ydb_Operations.OperationParams_OPERATION_MODE_UNSPECIFIED
	}
}
