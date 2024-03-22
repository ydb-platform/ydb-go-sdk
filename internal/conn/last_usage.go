package conn

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type lastUsage struct {
	locks atomic.Int64
	mu    xsync.RWMutex
	t     time.Time
	clock clockwork.Clock
}

func (l *lastUsage) Get() time.Time {
	if l.locks.CompareAndSwap(0, 1) {
		defer func() {
			l.locks.Add(-1)
		}()

		l.mu.RLock()
		defer l.mu.RUnlock()

		return l.t
	}

	return l.clock.Now()
}

func (l *lastUsage) Lock() (releaseFunc func()) {
	l.locks.Add(1)

	return sync.OnceFunc(func() {
		if l.locks.Add(-1) == 0 {
			l.mu.WithLock(func() {
				l.t = l.clock.Now()
			})
		}
	})
}
