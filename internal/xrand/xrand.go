package xrand

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"io"
	"math/rand"
	"sync"
	"time"
)

type Rand interface {
	Int64(max int64) int64
	Int(max int) int
	Shuffle(n int, swap func(i, j int))
}

type r struct {
	m *sync.Mutex
	r *rand.Rand
}

type option func(r *r)

func WithLock() option {
	return func(r *r) {
		r.m = &sync.Mutex{}
	}
}

func WithSeed(seed int64) option {
	return func(r *r) {
		r.r = rand.New(rand.NewSource(seed)) //nolint:gosec
	}
}

func New(opts ...option) Rand {
	r := &r{
		r: rand.New(rand.NewSource(time.Now().Unix())), //nolint:gosec
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	return r
}

// NewCryptoSeeded creates a pseudo-random generator with a seed obtained from
// crypto/rand. It is suitable for independently distributing choices made by
// different processes while retaining the efficient Rand implementation.
func NewCryptoSeeded(opts ...option) (Rand, error) {
	return newCryptoSeeded(cryptorand.Reader, opts...)
}

func newCryptoSeeded(reader io.Reader, opts ...option) (Rand, error) {
	var seed [8]byte
	if _, err := io.ReadFull(reader, seed[:]); err != nil {
		return nil, err
	}
	opts = append(opts, WithSeed(int64(binary.LittleEndian.Uint64(seed[:]))))

	return New(opts...), nil
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

func (r *r) Shuffle(n int, swap func(i, j int)) {
	if r.m != nil {
		r.m.Lock()
		defer r.m.Unlock()
	}

	r.r.Shuffle(n, swap)
}
