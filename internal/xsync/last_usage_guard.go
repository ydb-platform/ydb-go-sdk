package xsync

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type (
	LastUsage interface {
		Get() time.Time
		Start() (stop func())
	}
	lastUsage struct {
		locks atomic.Int64
		t     atomic.Pointer[time.Time]
		clock clockwork.Clock
	}
	lastUsageOption func(g *lastUsage)
)

func WithClock(clock clockwork.Clock) lastUsageOption {
	return func(g *lastUsage) {
		g.clock = clock
	}
}

func NewLastUsage(opts ...lastUsageOption) *lastUsage {
	lastUsage := &lastUsage{
		clock: clockwork.NewRealClock(),
	}
	for _, opt := range opts {
		opt(lastUsage)
	}

	now := lastUsage.clock.Now()

	lastUsage.t.Store(&now)

	return lastUsage
}

func (g *lastUsage) Get() time.Time {
	if g.locks.Load() == 0 {
		return *g.t.Load()
	}

	return g.clock.Now()
}

func (g *lastUsage) Start() (stop func()) {
	g.locks.Add(1)

	return sync.OnceFunc(func() {
		if g.locks.Add(-1) == 0 {
			now := g.clock.Now()
			g.t.Store(&now)
		}
	})
}
