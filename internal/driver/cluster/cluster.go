package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/endpoint"
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
	Stats(it func(endpoint.Endpoint, stats.Stats))
	Close() error
	Remove(ctx context.Context, e endpoint.Endpoint, wg ...option)
	SetExplorer(repeater repeater.Repeater)
	Force()
}

func New(
	dial func(context.Context, string, int) (*grpc.ClientConn, error),
	balancer balancer.Balancer,
) Cluster {
	return &cluster{
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
	if c.explorer != nil {
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
		if conn == nil {
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
	conn = c.balancer.Next()
	if conn == nil {
		return nil, ErrClusterEmpty
	}
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
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	addr := endpoint.Addr{e.Host, e.Port}
	info := info.Info{
		LoadFactor: e.LoadFactor,
		Local:      e.Local,
	}
	conn := conn.New(ctx, addr, c.dial, opt.connConfig)
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
	_, has := c.index[addr]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}
	entry := entry.Entry{Info: info}
	entry.Conn = conn
	entry.InsertInto(c.balancer)
	c.ready++
	wait = c.wait
	c.wait = nil
	c.index[addr] = entry
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(_ context.Context, ep endpoint.Endpoint, opts ...option) {
	opt := options{}
	for _, o := range opts {
		o(&opt)
	}
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	addr := endpoint.Addr{ep.Host, ep.Port}
	info := info.Info{
		LoadFactor: ep.LoadFactor,
		Local:      ep.Local,
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[addr]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}

	entry.Info = info
	if entry.Conn != nil {
		entry.Conn.Runtime().SetState(state.Online)
	}
	c.index[addr] = entry
	if entry.Handle != nil {
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
	if opt.wg != nil {
		defer opt.wg.Done()
	}

	addr := endpoint.Addr{e.Host, e.Port}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[addr]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}
	entry.RemoveFrom(c.balancer)
	c.ready--
	delete(c.index, addr)
	c.mu.Unlock()

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close()
	}
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
	if entry.Handle == nil {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrNilBalancerElement)
	}
	if !c.balancer.Contains(entry.Handle) {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrUnknownBalancerElement)
	}
	err = c.balancer.Pessimize(entry.Handle)
	if err == nil && c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, e := range c.index {
			if e.Conn != nil && e.Conn.Runtime().GetState() == state.Online {
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
	call := func(conn conn.Conn, info info.Info) {
		e := endpoint.Endpoint{
			Addr: endpoint.Addr{
				Host: conn.Addr().Host,
				Port: conn.Addr().Port,
			},
			LoadFactor: info.LoadFactor,
			Local:      info.Local,
		}
		s := conn.Runtime().Stats()
		it(e, s)
	}
	for _, entry := range c.index {
		if entry.Conn != nil {
			call(entry.Conn, entry.Info)
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
