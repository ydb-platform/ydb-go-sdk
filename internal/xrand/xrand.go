package xrand

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type Rand interface {
	Next() int64
	Shuffle(n int, swap func(i, j int))
}

type r struct {
	m      *sync.Mutex
	source rand.Source
	r      *rand.Rand
}

type option func(r *r)

func WithLock() option {
	return func(r *r) {
		r.m = &sync.Mutex{}
	}
}

func WithSource(source rand.Source) option {
	return func(r *r) {
		r.source = source
	}
}

func New(opts ...option) *r {
	r := &r{
		source: rand.NewSource(time.Now().Unix()),
	}
	for _, o := range opts {
		o(r)
	}
	//nolint: gosec
	r.r = rand.New(r.source)
	return r
}

func (r *r) Next() int64 {
	if r.m != nil {
		r.m.Lock()
		defer r.m.Unlock()
	}
	return r.r.Int63()
}

func (r *r) Shuffle(n int, swap func(i, j int)) {
	if r.m != nil {
		r.m.Lock()
		defer r.m.Unlock()
	}
	r.r.Shuffle(n, swap)
}

type roundRobinSource struct {
	max  int64
	next int64
}

var _ rand.Source = &roundRobinSource{}

func NewRoundRobinSource(max int64) *roundRobinSource {
	if max == 0 {
		max = math.MaxInt64
	}
	return &roundRobinSource{max: max}
}

func (r *roundRobinSource) Int63() int64 {
	r.next++
	r.next = r.next % r.max
	return r.next
}

func (r *roundRobinSource) Seed(seed int64) {
	r.next = seed
}

type randomSource struct {
	source rand.Source
	max    int64
}

var _ rand.Source = &randomSource{}

func NewRandomSource(max int64) *randomSource {
	if max == 0 {
		max = math.MaxInt64
	}
	return &randomSource{
		source: rand.NewSource(time.Now().Unix()),
		max:    max,
	}
}

func (r *randomSource) Int63() int64 {
	return r.source.Int63() % r.max
}

func (r *randomSource) Seed(seed int64) {
	r.source.Seed(seed)
}
