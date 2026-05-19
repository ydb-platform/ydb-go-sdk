package xsync

import "sync/atomic"

func (guard *lastUsage) Start() (stop func()) {
	guard.locks.Add(1)

	var stopped atomic.Bool

	return func() {
		if stopped.Swap(true) {
			return
		}
		if guard.locks.Add(-1) == 0 {
			now := guard.clock.Now()
			guard.t.Store(&now)
		}
	}
}
