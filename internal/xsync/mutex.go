package xsync

import (
	"sync"
)

type Mutex struct { //nolint:gocritic
	sync.Mutex
}

func (l *Mutex) WithLock(f func()) {
	l.Lock()
	defer l.Unlock()

	f()
}

type RWMutex struct { //nolint:gocritic
	sync.RWMutex
}

func (l *RWMutex) WithLock(f func()) {
	l.Lock()
	defer l.Unlock()

	f()
}

func (l *RWMutex) WithRLock(f func()) {
	l.RLock()
	defer l.RUnlock()

	f()
}
