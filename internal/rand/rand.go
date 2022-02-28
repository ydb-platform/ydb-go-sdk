package rand

import (
	"math"
	"math/rand"
	"sync"
)

type Rand interface {
	Int64(max int64) int64
	Int(max int) int
}

type r struct {
	r *rand.Rand
	m *sync.Mutex
}

type option func(r *r)

func WithLock() option {
	return func(r *r) {
		r.m = &sync.Mutex{}
	}
}

func New(opts ...option) Rand {
	r := &r{
		// nolint:gosec
		r: rand.New(rand.NewSource(math.MaxInt64)),
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *r) int64n(max int64) int64 {
	if r.m != nil {
		r.m.Lock()
		defer r.m.Unlock()
	}
	return r.r.Int63n(max)
}

func (r *r) Int64(max int64) int64 {
	return r.int64n(max)
}

func (r *r) Int(max int) int {
	return int(r.int64n(int64(max)))
}
