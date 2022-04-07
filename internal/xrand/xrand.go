package xrand

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
	m   *sync.Mutex
	max int64

	r *rand.Rand
}

type option func(r *r)

func WithLock() option {
	return func(r *r) {
		r.m = &sync.Mutex{}
	}
}

func WithMax(max int64) option {
	return func(r *r) {
		r.max = max
	}
}

func New(opts ...option) Rand {
	r := &r{
		max: math.MaxInt64,
	}
	for _, o := range opts {
		o(r)
	}
	// nolint:gosec
	r.r = rand.New(rand.NewSource(math.MaxInt64))
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
