package ydb

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type criterion interface {
	Best(a, b *conn) *conn
}

type connRuntimeCriterion struct {
}

func (t connRuntimeCriterion) Best(c1, c2 *conn) *conn {
	s1 := c1.runtime.stats(now)
	s2 := c2.runtime.stats(now)

	var (
		f1 float64
		f2 float64
	)
	if s1.ReqPerMinute > 0 {
		f1 = s1.ErrPerMinute / s1.ReqPerMinute
	}
	if s2.ReqPerMinute > 0 {
		f2 = s2.ErrPerMinute / s2.ReqPerMinute
	}
	if f1 == f2 {
		t := s1.AvgReqTime - s2.AvgReqTime
		if time.Duration(math.Abs(t)) > time.Second {
			if t < 0 {
				f1 = 0
				f2 = 1
			} else {
				f1 = 1
				f2 = 0
			}
		} else {
			f1 = s1.ReqPending
			f2 = s2.ReqPending
		}
	}
	if f1 < f2 {
		return c1
	}
	return c2
}

// p2c implements the "power of two choices" balancing algorithm.
// See https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf
type p2c struct {
	Source    rand.Source64
	Criterion criterion

	once sync.Once
	rand *rand.Rand

	conns connList
}

func (p *p2c) init() {
	p.once.Do(func() {
		if p.Criterion == nil {
			p.Criterion = new(connRuntimeCriterion)
		}
		if p.Source == nil {
			p.Source = rand.NewSource(0).(rand.Source64)
		}
		p.rand = rand.New(&lockedSource{src: p.Source})
	})
}

func (p *p2c) Next() *conn {
	p.init()

	n := len(p.conns)
	switch n {
	case 0:
		return nil
	case 1:
		return p.conns[0].conn
	}

	var (
		r1 uint64
		r2 uint64
	)
	const (
		maxRetries = 2
	)
	for i := 0; i <= maxRetries; i++ {
		rnd := p.rand.Uint64()
		r1 = (rnd >> 32) % uint64(n)
		r2 = (rnd & 0xffffffff) % uint64(n)
		if r1 != r2 {
			break
		}
	}
	c1 := p.conns[r1].conn
	c2 := p.conns[r2].conn

	return p.Criterion.Best(c1, c2)
}

func (p *p2c) Insert(conn *conn, info connInfo) balancerElement {
	el := p.conns.Insert(conn, info)
	return el
}

func (p *p2c) Update(x balancerElement, info connInfo) {
	el := x.(*connListElement)
	el.info = info
}

func (p *p2c) Remove(x balancerElement) {
	p.conns.Remove(x.(*connListElement))
}

type lockedSource struct {
	mu  sync.Mutex
	src rand.Source64
}

func (s *lockedSource) Int63() int64 {
	s.mu.Lock()
	x := s.src.Int63()
	s.mu.Unlock()
	return x
}
func (s *lockedSource) Uint64() uint64 {
	s.mu.Lock()
	x := s.src.Uint64()
	s.mu.Unlock()
	return x
}
func (s *lockedSource) Seed(seed int64) {
	s.mu.Lock()
	s.src.Seed(seed)
	s.mu.Unlock()
}
