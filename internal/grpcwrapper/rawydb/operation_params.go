package rawydb

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
)

type OperationParams struct {
	OperationMode OperationParamsMode

	OperationTimeout rawoptional.Duration
	CancelAfter      rawoptional.Duration
}

func (p *OperationParams) ToProto() *Ydb_Operations.OperationParams {
	res := &Ydb_Operations.OperationParams{
		OperationMode: p.OperationMode.ToProto(),
	}
	res.OperationTimeout = p.OperationTimeout.ToProto()
	res.CancelAfter = p.CancelAfter.ToProto()
	return res
}

type OperationParamsMode int

const (
	OperationParamsModeUnspecified OperationParamsMode = 0
	OperationParamsModeSync        OperationParamsMode = 1
	OperationParamsModeAsync       OperationParamsMode = 2
)

func (mode OperationParamsMode) ToProto() Ydb_Operations.OperationParams_OperationMode {
	return Ydb_Operations.OperationParams_OperationMode(mode)
}
