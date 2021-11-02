package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	public "github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/state"
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

	index map[uint32]entry.Entry

	mu     sync.RWMutex
	closed bool
}

func (c *cluster) Force() {
	c.explorer.Force()
}

func (c *cluster) SetExplorer(repeater repeater.Repeater) {
	c.explorer = repeater
}

type Cluster interface {
	Insert(ctx context.Context, endpoint endpoint.Endpoint, opts ...option)
	Update(ctx context.Context, endpoint endpoint.Endpoint, opts ...option)
	Get(ctx context.Context) (conn conn.Conn, err error)
	Pessimize(ctx context.Context, endpoint endpoint.Endpoint) error
	Close(ctx context.Context) error
	Remove(ctx context.Context, endpoint endpoint.Endpoint, wg ...option)
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
		index:    make(map[uint32]entry.Entry),
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

	index := c.index
	c.index = nil

	c.mu.Unlock()

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
	if e, ok := public.ContextEndpoint(ctx); ok {
		if conn, ok := c.index[e.NodeID()]; ok {
			return conn.Conn, nil
		}
	}

	conn = c.balancer.Next()
	if conn == nil {
		err = ErrClusterEmpty
	}
	onDone(conn.Endpoint(), err)
	return conn, err
}

type optionsHolder struct {
	wg         wg.WG
	connConfig conn.Config
}

type option func(options *optionsHolder)

func WithWG(wg wg.WG) option {
	return func(options *optionsHolder) {
		options.wg = wg
	}
}

func WithConnConfig(connConfig conn.Config) option {
	return func(options *optionsHolder) {
		options.connConfig = connConfig
	}
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e endpoint.Endpoint, opts ...option) {
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

	conn := conn.New(
		e,
		c.dial,
		holder.connConfig,
	)

	onDone := trace.DriverOnClusterInsert(c.trace, ctx, e)
	defer func() {
		onDone(conn.GetState())
	}()

	_, has := c.index[e.NodeID()]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}

	var wait chan struct{}
	defer func() {
		if wait != nil {
			close(wait)
		}
	}()

	entry := entry.Entry{Info: info.Info{LoadFactor: e.LoadFactor, Local: e.Local}}
	entry.Conn = conn
	entry.InsertInto(c.balancer)
	c.index[e.NodeID()] = entry
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(ctx context.Context, e endpoint.Endpoint, opts ...option) {
	onDone := trace.DriverOnClusterUpdate(c.trace, ctx, e)
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

	entry, has := c.index[e.NodeID()]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}
	if entry.Conn == nil {
		panic("ydb: cluster entry with nil conn")
	}

	defer func() {
		onDone(entry.Conn.GetState())
	}()

	entry.Info = info.Info{LoadFactor: e.LoadFactor, Local: e.Local}
	entry.Conn.SetState(ctx, state.Online)
	c.index[e.NodeID()] = entry
	if entry.Handle != nil {
		// entry.Handle may be nil when connection is being tracked.
		c.balancer.Update(entry.Handle, entry.Info)
	}
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(ctx context.Context, e endpoint.Endpoint, opts ...option) {
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

	entry, has := c.index[e.NodeID()]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}

	onDone := trace.DriverOnClusterRemove(c.trace, ctx, e)

	entry.RemoveFrom(c.balancer)
	delete(c.index, e.NodeID())
	c.mu.Unlock()

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close(ctx)
	}
	onDone(entry.Conn.GetState())
}

func (c *cluster) Pessimize(ctx context.Context, e endpoint.Endpoint) (err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrClusterClosed)
	}

	entry, has := c.index[e.NodeID()]
	if !has {
		return fmt.Errorf("cluster: pessimize failed: %w", ErrUnknownEndpoint)
	}
	if entry.Handle == nil {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrNilBalancerElement)
	}
	if !c.balancer.Contains(entry.Handle) {
		return fmt.Errorf("cluster: pessimize failed: %w", balancer.ErrUnknownBalancerElement)
	}
	entry.Conn.SetState(ctx, state.Banned)
	if c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, entry := range c.index {
			if entry.Conn != nil && entry.Conn.GetState() == state.Online {
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

func compareEndpoints(a, b endpoint.Endpoint) int {
	return int(int64(a.NodeID()) - int64(b.NodeID()))
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
