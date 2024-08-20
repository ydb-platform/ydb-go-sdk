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

func WithLock[T any](l interface {
	Lock()
	Unlock()
}, f func() T) T {
	l.Lock()
	defer l.Unlock()

	return f()
}

func WithRLock[T any](l interface {
	RLock()
	RUnlock()
}, f func() T) T {
	l.RLock()
	defer l.RUnlock()

	return f()
}
