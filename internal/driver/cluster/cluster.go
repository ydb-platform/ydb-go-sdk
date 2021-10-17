package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wg"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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
	dial     func(context.Context, string) (*grpc.ClientConn, error)
	balancer balancer.Balancer
	explorer repeater.Repeater

	index map[string]entry.Entry
	ready int
	wait  chan struct{}

	mu     sync.RWMutex
	closed bool
}

func (c *cluster) ConnStats(address string) (ok bool, _ stats.Stats) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.index[address]
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
	Insert(ctx context.Context, address string, opts ...option)
	Update(ctx context.Context, address string, opts ...option)
	Get(ctx context.Context) (conn conn.Conn, err error)
	Pessimize(ctx context.Context, address string) error
	ConnStats(address string) (ok bool, stats stats.Stats)
	Close(ctx context.Context) error
	Remove(ctx context.Context, address string, wg ...option)
	SetExplorer(repeater repeater.Repeater)
	Force()
}

func New(
	trace trace.Driver,
	dial func(context.Context, string) (*grpc.ClientConn, error),
	balancer balancer.Balancer,
) Cluster {
	return &cluster{
		trace:    trace,
		index:    make(map[string]entry.Entry),
		dial:     dial,
		balancer: balancer,
	}
}

func (c *cluster) Close(ctx context.Context) (err error) {
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

	c.mu.Unlock()

	if wait != nil {
		close(wait)
	}
	for _, entry := range index {
		if entry.Conn != nil {
			_ = entry.Conn.Close(ctx)
		}
	}

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
	if conn == nil {
		onDone("", ErrClusterEmpty)
		return nil, ErrClusterEmpty
	}
	onDone(conn.Address(), nil)
	return conn, nil
}

type optionsHolder struct {
	wg         wg.WG
	info       info.Info
	connConfig conn.Config
}

type option func(options *optionsHolder)

func WithWG(wg wg.WG) option {
	return func(options *optionsHolder) {
		options.wg = wg
	}
}

func WithInfo(info info.Info) option {
	return func(options *optionsHolder) {
		options.info = info
	}
}

func WithConnConfig(connConfig conn.Config) option {
	return func(options *optionsHolder) {
		options.connConfig = connConfig
	}
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, address string, opts ...option) {
	holder := optionsHolder{}
	for _, o := range opts {
		o(&holder)
	}
	if holder.wg != nil {
		defer holder.wg.Done()
	}

	conn := conn.New(ctx, address, c.dial, holder.connConfig)
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

	_, has := c.index[address]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}

	onDone := trace.DriverOnClusterInsert(c.trace, ctx, address)

	entry := entry.Entry{Info: holder.info}
	entry.Conn = conn
	entry.InsertInto(c.balancer)
	c.ready++
	wait = c.wait
	c.wait = nil

	c.index[address] = entry

	onDone(
		conn.Runtime().GetState(),
		func() trace.Location {
			if holder.info.Local {
				return trace.LocationLocal
			}
			return trace.LocationRemote
		}(),
	)
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(ctx context.Context, address string, opts ...option) {
	onDone := trace.DriverOnClusterUpdate(c.trace, ctx, address)
	holder := optionsHolder{}
	for _, o := range opts {
		o(&holder)
	}
	if holder.wg != nil {
		defer holder.wg.Done()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[address]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}
	if entry.Conn == nil {
		panic("ydb: cluster entry with nil conn")
	}

	defer func() {
		onDone(entry.Conn.Runtime().GetState())
	}()

	entry.Info = holder.info
	entry.Conn.Runtime().SetState(ctx, state.Online)
	c.index[address] = entry
	if entry.Handle != nil {
		// entry.Handle may be nil when connection is being tracked.
		c.balancer.Update(entry.Handle, holder.info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(ctx context.Context, address string, opts ...option) {
	holder := optionsHolder{}
	for _, o := range opts {
		o(&holder)
	}
	if holder.wg != nil {
		defer holder.wg.Done()
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[address]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}

	onDone := trace.DriverOnClusterRemove(c.trace, ctx, address)

	entry.RemoveFrom(c.balancer)
	c.ready--
	delete(c.index, address)
	c.mu.Unlock()

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close(ctx)
	}
	onDone(entry.Conn.Runtime().GetState())
}

func (c *cluster) Pessimize(ctx context.Context, address string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrClusterClosed)
	}

	entry, has := c.index[address]
	if !has {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrUnknownEndpoint)
	}
	if entry.Handle == nil {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrNilBalancerElement)
	}
	if !c.balancer.Contains(entry.Handle) {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrUnknownBalancerElement)
	}
	err = c.balancer.Pessimize(ctx, entry.Handle)
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

// c.mu read lock must be held.
// nolint:unused
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
