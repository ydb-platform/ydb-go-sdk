package stats

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/state"
	"time"
)

type Stats struct {
	State        state.State
	OpStarted    uint64
	OpFailed     uint64
	OpSucceed    uint64
	OpPerMinute  float64
	ErrPerMinute float64
	AvgOpTime    time.Duration
}

func (c Stats) OpPending() uint64 {
	return c.OpStarted - (c.OpFailed + c.OpSucceed)
}
