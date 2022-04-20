package rr

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

var randomSources = xrand.New(xrand.WithLock())

type baseBalancer struct {
	conns []conn.Conn

	m           sync.Mutex
	needRefresh chan struct{}
}

func (r *baseBalancer) NeedRefresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-r.needRefresh:
		return true
	}
}

func (r *baseBalancer) isNeedRefreshClosed() bool {
	select {
	case <-r.needRefresh:
		return true
	default:
		return false
	}
}

func (r *baseBalancer) checkNeedRefresh(failedConns *int) {
	connsCount := len(r.conns)
	if connsCount > 0 && *failedConns <= connsCount/2 {
		return
	}

	r.m.Lock()
	defer r.m.Unlock()

	if r.isNeedRefreshClosed() {
		return
	}

	close(r.needRefresh)
}

type roundRobin struct {
	baseBalancer

	next int64
}

func (r *roundRobin) Create(conns []conn.Conn) balancer.Balancer {
	return RoundRobin(conns)
}

func RoundRobin(conns []conn.Conn) balancer.Balancer {
	return RoundRobinWithStartPosition(conns, int(randomSources.Int64(math.MaxInt64)))
}

func RoundRobinWithStartPosition(conns []conn.Conn, index int) balancer.Balancer {
	return &roundRobin{
		baseBalancer: baseBalancer{
			conns: conns,
		},
		// random start need to prevent overload first nodes
		next: int64(index),
	}
}

func (r *roundRobin) Next(_ context.Context, allowBanned bool) conn.Conn {
	connCount := len(r.conns)

	failedConns := 0
	defer r.checkNeedRefresh(&failedConns)

	startIndex := r.nextStartIndex()
	for i := 0; i < connCount; i++ {
		connIndex := (startIndex + i) % connCount
		c := r.conns[connIndex]
		if balancer.IsOkConnection(c, allowBanned) {
			return c
		}
		failedConns++
	}

	return nil
}

func (r *roundRobin) nextStartIndex() int {
	res := atomic.AddInt64(&r.next, 1) % int64(len(r.conns))
	if res < 0 {
		atomic.CompareAndSwapInt64(&r.next, res, 0)
		return r.nextStartIndex()
	}
	index := int(res) % len(r.conns)
	return index
}

type randomChoice struct {
	baseBalancer

	rand xrand.Rand
}

func RandomChoice(conns []conn.Conn) balancer.Balancer {
	return &randomChoice{
		baseBalancer: baseBalancer{conns: conns},
		rand:         xrand.New(xrand.WithLock(), xrand.WithSource(randomSources.Int64(math.MaxInt64))),
	}
}

func (r *randomChoice) Create(conns []conn.Conn) balancer.Balancer {
	return RandomChoice(conns)
}

func (r *randomChoice) Next(_ context.Context, allowBanned bool) conn.Conn {
	connCount := len(r.conns)

	if connCount == 0 {
		// return for empty list need for prevent panic in fast path
		return nil
	}

	// fast path
	if c := r.conns[r.rand.Int(connCount)]; balancer.IsOkConnection(c, allowBanned) {
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

	failedConns := 0
	defer r.checkNeedRefresh(&failedConns)

	for _, index := range indexes {
		c := r.conns[index]
		if balancer.IsOkConnection(c, allowBanned) {
			return c
		}
		failedConns++
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
