//go:build !go1.21
// +build !go1.21

package xsync

import (
	"sync"
)

func (guard *lastUsage) Start() (stop func()) {
	guard.locks.Add(1)

	var (
		once  sync.Once
		valid bool
		p     any
	)
	f := func() {
		if guard.locks.Add(-1) == 0 {
			now := guard.clock.Now()
			guard.t.Store(&now)
		}
	}
	// Construct the inner closure just once to reduce costs on the fast path.
	g := func() {
		defer func() {
			p = recover()
			if !valid {
				// Re-panic immediately so on the first call the user gets a
				// complete stack trace into f.
				panic(p)
			}
		}()
		f()
		f = nil      // Do not keep f alive after invoking it.
		valid = true // Set only if f does not panic.
	}
	return func() {
		once.Do(g)
		if !valid {
			panic(p)
		}
	}
}
