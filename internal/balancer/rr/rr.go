package rr

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

var randomSources = xrand.New(xrand.WithLock())

type baseBalancer struct {
	mu    sync.RWMutex
	conns list.List
}

func (r *baseBalancer) Contains(x balancer.Element) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if x == nil {
		return false
	}

	el, ok := x.(*list.Element)
	if !ok {
		return false
	}

	return r.conns.Contains(el)
}

func (r *baseBalancer) Insert(conn conn.Conn) balancer.Element {
	r.mu.Lock()
	defer r.mu.Unlock()

	e := r.conns.Insert(conn)
	return e
}

func (r *baseBalancer) Remove(x balancer.Element) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	el := x.(*list.Element)
	r.conns.Remove(el)
	return true
}

type roundRobin struct {
	baseBalancer

	next uint64
}

func (r *roundRobin) Create() balancer.Balancer {
	return RoundRobin()
}

func RoundRobin() balancer.Balancer {
	return &roundRobin{
		// random start need to prevent overload first nodes
		next: uint64(randomSources.Int64(math.MaxInt64)),
	}
}

func (r *roundRobin) Next() conn.Conn {
	r.mu.RLock()
	defer r.mu.RUnlock()

	connCount := len(r.conns)

	for _, bannedIsOk := range []bool{false, true} {
		for i := 0; i < connCount; i++ {
			index := int(atomic.AddUint64(&r.next, 1) % uint64(connCount))
			c := r.conns[index].Conn
			if isOkConnection(c, bannedIsOk) {
				return c
			}
		}
	}

	return nil
}

type randomChoice struct {
	baseBalancer

	rand xrand.Rand
}

func RandomChoice() balancer.Balancer {
	return &randomChoice{
		rand: xrand.New(xrand.WithLock(), xrand.WithSource(randomSources.Int64(math.MaxInt64))),
	}
}

func (r *randomChoice) Create() balancer.Balancer {
	return RandomChoice()
}

func (r *randomChoice) Next() conn.Conn {
	r.mu.RLock()
	defer r.mu.RUnlock()

	connCount := len(r.conns)

	if connCount == 0 {
		// return for empty list need for prevent panic in fast path
		return nil
	}

	// fast path
	if c := r.conns[r.rand.Int(connCount)].Conn; isOkConnection(c, false) {
		return c
	}

	// shuffled indexes slices need for guarantee about every connection will check
	indexes := make([]int, connCount)
	for index := range indexes {
		indexes[index] = index
	}
	r.rand.Shuffle(connCount, func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	for _, bannedIsOk := range []bool{false, true} {
		for _, index := range indexes {
			c := r.conns[index].Conn
			if isOkConnection(c, bannedIsOk) {
				return c
			}
		}
	}

	return nil
}

func isOkConnection(c conn.Conn, bannedIsOk bool) bool {
	state := c.GetState()
	if state == conn.Online || state == conn.Created {
		return true
	}
	if bannedIsOk && state == conn.Banned {
		return true
	}
	return false
}
