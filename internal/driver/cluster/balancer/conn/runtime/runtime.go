package runtime

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/state"
	stats2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/stats"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/timeutil"
)

const (
	statsDuration = time.Minute
	statsBuckets  = 12
)

type Runtime interface {
	Stats() stats2.Stats
	GetState() (s state.State)
	SetState(s state.State)
	OperationStart(start time.Time)
	OperationDone(start, end time.Time, err error)
	StreamStart(now time.Time)
	StreamRecv(now time.Time)
	StreamDone(now time.Time, err error)
	SetOpStarted(id uint64)
}

type runtime struct {
	mu           sync.RWMutex
	state        state.State
	offlineCount uint64
	opStarted    uint64
	opSucceed    uint64
	opFailed     uint64
	opTime       *stats2.Series
	opRate       *stats2.Series
	errRate      *stats2.Series
}

func New() Runtime {
	return &runtime{
		opTime:  stats2.NewSeries(statsDuration, statsBuckets),
		opRate:  stats2.NewSeries(statsDuration, statsBuckets),
		errRate: stats2.NewSeries(statsDuration, statsBuckets),
	}
}

func (c *runtime) Stats() stats2.Stats {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := timeutil.Now()

	r := stats2.Stats{
		State:        c.state,
		OpStarted:    c.opStarted,
		OpSucceed:    c.opSucceed,
		OpFailed:     c.opFailed,
		OpPerMinute:  c.opRate.SumPer(now, time.Minute),
		ErrPerMinute: c.errRate.SumPer(now, time.Minute),
	}
	if rtSum, rtCnt := c.opTime.Get(now); rtCnt > 0 {
		r.AvgOpTime = time.Duration(rtSum / float64(rtCnt))
	}

	return r
}

func (c *runtime) SetState(s state.State) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = s
	if s == state.Offline {
		c.offlineCount++
	}
}

func (c *runtime) GetState() (s state.State) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

func (c *runtime) OperationStart(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.opStarted++
	c.opRate.Add(start, 1)
}

func (c *runtime) OperationDone(start, end time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err != nil {
		c.opFailed++
		c.errRate.Add(end, 1)
	} else {
		c.opSucceed++
	}
	c.opTime.Add(end, float64(end.Sub(start)))
}

func (c *runtime) StreamStart(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *runtime) StreamRecv(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opRate.Add(now, 1)
}

func (c *runtime) StreamDone(now time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.errRate.Add(now, 1)
	}
}

func (c *runtime) SetOpStarted(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opStarted = id
}
