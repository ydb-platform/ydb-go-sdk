package conn

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type lastUsageGuard struct {
	locks atomic.Int64
	t     atomic.Pointer[time.Time]
	clock clockwork.Clock
}

func newLastUsage(clock clockwork.Clock) *lastUsageGuard {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}

	now := clock.Now()

	usage := &lastUsageGuard{
		clock: clock,
	}

	usage.t.Store(&now)

	return usage
}

func (g *lastUsageGuard) Get() time.Time {
	if g.locks.Load() == 0 {
		return *g.t.Load()
	}

	return g.clock.Now()
}

func (g *lastUsageGuard) Start() (stop func()) {
	g.locks.Add(1)

	return sync.OnceFunc(func() {
		if g.locks.Add(-1) == 0 {
			now := g.clock.Now()
			g.t.Store(&now)
		}
	})
}
