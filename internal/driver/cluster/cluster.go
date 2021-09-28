package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wg"

	"google.golang.org/grpc"
)

const (
	MaxGetConnTimeout = 10 * time.Second
	//ConnResetOfflineRate = uint64(10)
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = errors.New("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = errors.New("cluster empty")

	// ErrUnknownEndpoint returned when no connections left in cluster.
	ErrUnknownEndpoint = errors.New("unknown endpoint")
)

type cluster struct {
	trace    trace.Driver
	dial     func(context.Context, string, int) (*grpc.ClientConn, error)
	balancer balancer.Balancer
	explorer repeater.Repeater

	mu    sync.RWMutex
	once  sync.Once
	index map[endpoint.Addr]entry.Entry
	ready int
	wait  chan struct{}

	closed bool
}

func (c *cluster) ConnStats(addr endpoint.Addr) (ok bool, _ stats.Stats) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.index[addr]
	if !ok {
		return false, stats.Stats{}
	}
	return true, entry.Conn.Runtime().Stats()
}

func (c *cluster) Force() {
	c.explorer.Force()
}

func (c *cluster) SetExplorer(repeater repeater.Repeater) {
	c.explorer = repeater
}

type Cluster interface {
	Insert(ctx context.Context, e endpoint.Endpoint, opts ...option)
	Update(ctx context.Context, e endpoint.Endpoint, opts ...option)
	Get(ctx context.Context) (conn conn.Conn, err error)
	Pessimize(a endpoint.Addr) error
	ConnStats(addr endpoint.Addr) (ok bool, stats stats.Stats)
	Stats(it func(endpoint.Endpoint, stats.Stats))
	Close() error
	Remove(ctx context.Context, e endpoint.Endpoint, wg ...option)
	SetExplorer(repeater repeater.Repeater)
	Force()
}

func New(
	trace trace.Driver,
	dial func(context.Context, string, int) (*grpc.ClientConn, error),
	balancer balancer.Balancer,
) Cluster {
	return &cluster{
		trace:    trace,
		index:    make(map[endpoint.Addr]entry.Entry),
		dial:     dial,
		balancer: balancer,
	}
}

func (c *cluster) Close() (err error) {
	var dummy bool
	c.once.Do(func() {
		dummy = true
	})
	if dummy {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if !assert.IsNil(c.explorer) {
		c.explorer.Stop()
	}
	c.closed = true

	wait := c.wait
	c.wait = nil

	index := c.index
	c.index = nil

	//c.trackerCancel()
	//
	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, entry := range index {
		conn := entry.Conn
		if assert.IsNil(conn) {
			continue
		}
		_ = conn.Close()
	}

	//<-c.trackerDone

	return
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (conn conn.Conn, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, ErrClusterClosed
	}
	onDone := trace.DriverOnClusterGet(c.trace, ctx)
	conn = c.balancer.Next()
	if assert.IsNil(conn) {
		onDone(nil, ErrClusterEmpty)
		return nil, ErrClusterEmpty
	}
	onDone(conn.Addr(), nil)
	return conn, nil
}

type options struct {
	wg         wg.WG
	connConfig conn.Config
}

type option func(options *options)

func WithWG(wg wg.WG) option {
	return func(options *options) {
		options.wg = wg
	}
}

func WithConnConfig(connConfig conn.Config) option {
	return func(options *options) {
		options.connConfig = connConfig
	}
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e endpoint.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if !assert.IsNil(opt.wg) {
		defer opt.wg.Done()
	}

	info := info.Info{
		LoadFactor: e.LoadFactor,
		Local:      e.Local,
	}
	conn := conn.New(ctx, e.Addr, c.dial, opt.connConfig)
	var wait chan struct{}
	defer func() {
		if wait != nil {
			close(wait)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	_, has := c.index[e.Addr]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}

	onDone := trace.DriverOnClusterInsert(c.trace, e.Addr)

	entry := entry.Entry{Info: info}
	entry.Conn = conn
	entry.InsertInto(c.balancer)
	c.ready++
	wait = c.wait
	c.wait = nil
	c.index[e.Addr] = entry

	onDone(len(c.index), conn.Runtime().GetState())
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(_ context.Context, e endpoint.Endpoint, opts ...option) {
	onDone := trace.DriverOnClusterUpdate(c.trace, e.Addr)
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if !assert.IsNil(opt.wg) {
		defer opt.wg.Done()
	}

	info := info.Info{
		LoadFactor: e.LoadFactor,
		Local:      e.Local,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[e.Addr]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}
	if assert.IsNil(entry.Conn) {
		panic("ydb: cluster entry with nil conn")
	}

	defer func() {
		onDone(entry.Conn.Runtime().GetState())
	}()

	entry.Info = info
	entry.Conn.Runtime().SetState(state.Online)
	c.index[e.Addr] = entry
	if !assert.IsNil(entry.Handle) {
		// entry.Handle may be nil when connection is being tracked.
		c.balancer.Update(entry.Handle, info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(_ context.Context, e endpoint.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if !assert.IsNil(opt.wg) {
		defer opt.wg.Done()
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[e.Addr]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}

	onDone := trace.DriverOnClusterRemove(c.trace, e.Addr)

	entry.RemoveFrom(c.balancer)
	c.ready--
	delete(c.index, e.Addr)
	l := len(c.index)
	c.mu.Unlock()

	if !assert.IsNil(entry.Conn) {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close()
	}
	onDone(l, entry.Conn.Runtime().GetState())

	trace.DriverOnConnDrop(c.trace, e.Addr, entry.Conn.Runtime().GetState())
}

func (c *cluster) Pessimize(addr endpoint.Addr) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrClusterClosed)
	}

	entry, has := c.index[addr]
	if !has {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrUnknownEndpoint)
	}
	if assert.IsNil(entry.Handle) {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrNilBalancerElement)
	}
	if !c.balancer.Contains(entry.Handle) {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrUnknownBalancerElement)
	}
	err = c.balancer.Pessimize(entry.Handle)
	if err == nil && !assert.IsNil(c.explorer) {
		// count ratio (banned/all)
		online := 0
		for _, e := range c.index {
			if !assert.IsNil(e.Conn) && e.Conn.Runtime().GetState() == state.Online {
				online++
			}
		}
		// more than half connections banned - re-discover now
		if online*2 < len(c.index) {
			c.explorer.Force()
		}
	}
	return err
}

func (c *cluster) Stats(it func(endpoint.Endpoint, stats.Stats)) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return
	}
	for _, entry := range c.index {
		if !assert.IsNil(entry.Conn) {
			it(
				endpoint.Endpoint{
					Addr: endpoint.Addr{
						Host: entry.Conn.Addr().Host,
						Port: entry.Conn.Addr().Port,
					},
					LoadFactor: entry.Info.LoadFactor,
					Local:      entry.Info.Local,
				},
				entry.Conn.Runtime().Stats(),
			)
		}
	}
}

// c.mu read lock must be held.
func (c *cluster) await() func() <-chan struct{} {
	prev := c.wait
	return func() <-chan struct{} {
		c.mu.RLock()
		wait := c.wait
		c.mu.RUnlock()
		if wait != prev {
			return wait
		}

		c.mu.Lock()
		wait = c.wait
		if wait != prev {
			c.mu.Unlock()
			return wait
		}
		wait = make(chan struct{})
		c.wait = wait
		c.mu.Unlock()

		return wait
	}
}

func compareEndpoints(a, b endpoint.Endpoint) int {
	if c := strings.Compare(a.Host, b.Host); c != 0 {
		return c
	}
	if c := a.Port - b.Port; c != 0 {
		return c
	}
	return 0
}

func SortEndpoints(es []endpoint.Endpoint) {
	sort.Slice(es, func(i, j int) bool {
		return compareEndpoints(es[i], es[j]) < 0
	})
}

func DiffEndpoints(curr, next []endpoint.Endpoint, eq, add, del func(i, j int)) {
	diffslice(
		len(curr),
		len(next),
		func(i, j int) int {
			return compareEndpoints(curr[i], next[j])
		},
		func(i, j int) {
			eq(i, j)
		},
		func(i, j int) {
			add(i, j)
		},
		func(i, j int) {
			del(i, j)
		},
	)
}

func diffslice(a, b int, cmp func(i, j int) int, eq, add, del func(i, j int)) {
	var i, j int
	for i < a && j < b {
		c := cmp(i, j)
		switch {
		case c < 0:
			del(i, j)
			i++
		case c > 0:
			add(i, j)
			j++
		default:
			eq(i, j)
			i++
			j++
		}
	}
	for ; i < a; i++ {
		del(i, j)
	}
	for ; j < b; j++ {
		add(i, j)
	}
}
