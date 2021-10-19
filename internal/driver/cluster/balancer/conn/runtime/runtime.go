package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/series"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
)

const (
	statsDuration = time.Minute
	statsBuckets  = 12
)

type Runtime interface {
	Stats() stats.Stats
	GetState() (s state.State)
	Location() trace.Location
	SetState(ctx context.Context, s state.State)
	OperationStart(start time.Time)
	OperationDone(start, end time.Time, err error)
	StreamStart(now time.Time)
	StreamRecv(now time.Time)
	StreamDone(now time.Time, err error)
	SetOpStarted(id uint64)
}

type Addr interface {
}

type runtime struct {
	mu        sync.RWMutex
	address   string
	location  trace.Location
	trace     trace.Driver
	state     state.State
	opStarted uint64
	opSucceed uint64
	opFailed  uint64
	opTime    *series.Series
	opRate    *series.Series
	errRate   *series.Series
}

func (r *runtime) Location() trace.Location {
	return r.location
}

func New(trace trace.Driver, address string, location trace.Location) Runtime {
	return &runtime{
		trace:    trace,
		address:  address,
		location: location,
		state:    state.Unknown,
		opTime:   series.NewSeries(statsDuration, statsBuckets),
		opRate:   series.NewSeries(statsDuration, statsBuckets),
		errRate:  series.NewSeries(statsDuration, statsBuckets),
	}
}

func (r *runtime) Stats() stats.Stats {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := timeutil.Now()

	s := stats.Stats{
		State:        r.state,
		OpStarted:    r.opStarted,
		OpSucceed:    r.opSucceed,
		OpFailed:     r.opFailed,
		OpPerMinute:  r.opRate.SumPer(now, time.Minute),
		ErrPerMinute: r.errRate.SumPer(now, time.Minute),
	}
	if rtSum, rtCnt := r.opTime.Get(now); rtCnt > 0 {
		s.AvgOpTime = time.Duration(rtSum / float64(rtCnt))
	}

	return s
}

func (r *runtime) SetState(ctx context.Context, s state.State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	onDone := trace.DriverOnConnStateChange(r.trace, ctx, r.address, r.location, r.state)
	r.state = s
	onDone(r.state)
}

func (r *runtime) GetState() (s state.State) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *runtime) OperationStart(start time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.opStarted++
	r.opRate.Add(start, 1)
}

func (r *runtime) OperationDone(start, end time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err != nil {
		r.opFailed++
		r.errRate.Add(end, 1)
	} else {
		r.opSucceed++
	}
	r.opTime.Add(end, float64(end.Sub(start)))
}

func (r *runtime) StreamStart(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.opRate.Add(now, 1)
}

func (r *runtime) StreamRecv(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.opRate.Add(now, 1)
}

func (r *runtime) StreamDone(now time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.errRate.Add(now, 1)
	}
}

func (r *runtime) SetOpStarted(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.opStarted = id
}
