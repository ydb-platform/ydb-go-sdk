package rr

import (
	"container/heap"
	"math"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ibalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/list"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/rand"
)

// roundRobin is an implementation of weighted round-robin balancing algorithm.
//
// It relies on connection's load factor (usually obtained by discovery
// routine – that is, not a runtime metric) and interprets it as inversion of
// weight.
type roundRobin struct {
	min   float32
	max   float32
	belt  []int
	next  int32
	conns list.List
}

func RoundRobin() ibalancer.Balancer {
	return &roundRobin{}
}

func RandomChoice() ibalancer.Balancer {
	return &randomChoice{}
}

type randomChoice struct {
	roundRobin
	m sync.Mutex
}

func (r *roundRobin) Next() conn.Conn {
	if n := len(r.conns); n == 0 {
		return nil
	}
	d := int(atomic.AddInt32(&r.next, 1)) % len(r.belt)
	i := r.belt[d]
	return r.conns[i].Conn
}

func (r *randomChoice) Next() conn.Conn {
	if n := len(r.conns); n == 0 {
		return nil
	}
	r.m.Lock()
	i := r.belt[rand.Int(len(r.belt))]
	r.m.Unlock()
	return r.conns[i].Conn
}

func (r *roundRobin) Insert(conn conn.Conn) ibalancer.Element {
	e := r.conns.Insert(conn)
	r.updateMinMax(e.Conn)
	r.belt = r.distribute()
	return e
}

func (r *roundRobin) Update(el ibalancer.Element, info info.Info) {
	e := el.(*list.Element)
	e.Info = info
	r.updateMinMax(e.Conn)
	r.belt = r.distribute()
}

func (r *roundRobin) Remove(x ibalancer.Element) {
	el := x.(*list.Element)
	r.conns.Remove(el)
	r.inspectMinMax(el.Info)
	r.belt = r.distribute()
}

func (r *roundRobin) Contains(x ibalancer.Element) bool {
	if x == nil {
		return false
	}
	el, ok := x.(*list.Element)
	if !ok {
		return false
	}
	return r.conns.Contains(el)
}

func (r *roundRobin) updateMinMax(cc conn.Conn) {
	if len(r.conns) == 1 {
		r.min = cc.Endpoint().LoadFactor()
		r.max = cc.Endpoint().LoadFactor()
		return
	}
	if cc.Endpoint().LoadFactor() < r.min {
		r.min = cc.Endpoint().LoadFactor()
	}
	if cc.Endpoint().LoadFactor() > r.max {
		r.max = cc.Endpoint().LoadFactor()
	}
}

func (r *roundRobin) inspectMinMax(info info.Info) {
	if r.min != info.LoadFactor && r.max != info.LoadFactor {
		return
	}
	var def bool
	for _, x := range r.conns {
		load := x.Info.LoadFactor
		if !def {
			r.min = load
			r.max = load
			def = true
		}
		if load < r.min {
			r.min = load
		}
		if load > r.max {
			r.max = load
		}
	}
}

func (r *roundRobin) distribute() []int {
	return r.spread(distribution(
		r.min, int32(len(r.conns)),
		r.max, 1,
	))
}

func (r *roundRobin) spread(f func(float32) int32) []int {
	var (
		dist  = make([]int32, 0, len(r.conns))
		index = make([]int, 0, len(r.conns))
	)
	fill := func(state state.State) (filled bool) {
		for _, x := range r.conns {
			if x.Conn.GetState() == state {
				d := f(x.Info.LoadFactor)
				dist = append(dist, d)
				index = append(index, x.Index)
				filled = true
			}
		}
		return filled
	}
	for _, s := range [...]state.State{
		state.Created,
		state.Online,
		state.Banned,
		state.Offline,
		state.Destroyed,
	} {
		if fill(s) {
			return genBelt(index, dist)
		}
	}
	return nil
}

func genBelt(index []int, weight []int32) (r []int) {
	h := make(distItemsHeap, len(weight))
	for i, w := range weight {
		h[i] = newDistItem(index[i], w)
	}
	heap.Init(&h)
	for len(h) > 0 {
		x := heap.Pop(&h).(*distItem)
		r = append(r, x.index())
		if x.tick() {
			heap.Push(&h, x)
		}
	}
	return
}

func distribution(x1 float32, y1 int32, x2 float32, y2 int32) (f func(float32) int32) {
	if x1 == x2 {
		f = func(float32) int32 { return 1 }
	} else {
		a := float32(y2-y1) / (x2 - x1)
		b := float32(y1) - a*x1
		f = func(x float32) int32 {
			return int32(math.Round(float64(a*x + b)))
		}
	}
	return f
}

type distItem struct {
	i     int
	step  float64
	value float64
}

// newDistItem creates new distribution item.
// w must be greater than zero.
func newDistItem(i int, w int32) *distItem {
	step := 1 / float64(w)
	return &distItem{
		i:     i,
		step:  step,
		value: step,
	}
}

func (x *distItem) tick() bool {
	x.value += x.step
	return x.value <= 1
}

func (x *distItem) index() int {
	return x.i
}

type distItemsHeap []*distItem

func (h distItemsHeap) Len() int { return len(h) }

func (h distItemsHeap) Less(i, j int) bool { return h[i].value < h[j].value }

func (h distItemsHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *distItemsHeap) Push(x interface{}) {
	*h = append(*h, x.(*distItem))
}

func (h *distItemsHeap) Pop() interface{} {
	p := *h
	n := len(p)
	x := p[n-1]
	*h = p[:n-1]
	return x
}
