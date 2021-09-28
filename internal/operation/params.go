package operation

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"time"
)

type Params struct {
	Timeout     time.Duration
	CancelAfter time.Duration
	Mode        Mode
}

type ModeInterface interface {
	String() string
}

func (p Params) GetTimeout() time.Duration {
	return p.Timeout
}

func (p Params) GetCancelAfter() time.Duration {
	return p.CancelAfter
}

func (p Params) GetMode() string {
	return p.Mode.String()
}

func (p Params) Empty() bool {
	return p.Timeout == 0 && p.CancelAfter == 0 && p.Mode == 0
}

func (p Params) toYDB() *Ydb_Operations.OperationParams {
	if p.Empty() {
		return nil
	}
	return &Ydb_Operations.OperationParams{
		OperationMode:    p.Mode.toYDB(),
		OperationTimeout: timeoutParam(p.Timeout),
		CancelAfter:      timeoutParam(p.CancelAfter),
	}
}

func SetOperationParams(req interface{}, params Params) {
	x, ok := req.(interface {
		SetOperationParams(*Ydb_Operations.OperationParams)
	})
	if !ok {
		return
	}
	x.SetOperationParams(params.toYDB())
}
