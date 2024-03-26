package conn

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type lastUsage struct {
	locks atomic.Int64
	t     atomic.Pointer[time.Time]
	clock clockwork.Clock
}

func newLastUsage(clock clockwork.Clock) *lastUsage {
	if clock == nil {
		clock = clockwork.NewRealClock()
	}
	now := clock.Now()
	usage := &lastUsage{
		clock: clock,
	}
	usage.t.Store(&now)

	return usage
}

func (l *lastUsage) Get() time.Time {
	if l.locks.Load() == 0 {
		return *l.t.Load()
	}

	return l.clock.Now()
}

func (l *lastUsage) Start() (stop func()) {
	l.locks.Add(1)

	return sync.OnceFunc(func() {
		if l.locks.Add(-1) == 0 {
			now := l.clock.Now()
			l.t.Store(&now)
		}
	})
}
