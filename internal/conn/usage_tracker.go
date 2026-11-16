package conn

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

type usageTracker struct {
	mu     sync.Mutex
	clock  clockwork.Clock
	active int
	last   time.Time
}

func newUsageTracker(clock clockwork.Clock) *usageTracker {
	return &usageTracker{
		clock: clock,
		last:  clock.Now(),
	}
}

func (u *usageTracker) start() func() {
	u.mu.Lock()
	u.active++
	u.mu.Unlock()

	var stopped atomic.Bool

	return func() {
		if stopped.Swap(true) {
			return
		}

		u.mu.Lock()
		defer u.mu.Unlock()

		u.active--
		if u.active == 0 {
			u.last = u.clock.Now()
		}
	}
}

func (u *usageTracker) ifIdle(ttl time.Duration, f func()) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.active > 0 || u.clock.Now().Sub(u.last) <= ttl {
		return
	}

	f()
}
