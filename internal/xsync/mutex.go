package xsync

import "sync"

type Mutex struct {
	sync.Mutex
}

func (l *Mutex) WithLock(f func()) {
	l.Lock()
	defer l.Unlock()

	f()
}

type RWMutex struct {
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
