package cluster

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint/info"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = errors.New("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = errors.New("cluster empty")

	// ErrUnknownEndpoint returned when no connections left in cluster.
	ErrUnknownEndpoint = errors.New("unknown endpoint")

	// ErrNilBalancerElement returned when requested on a nil Balancer element.
	ErrNilBalancerElement = errors.New("nil balancer element")
	// ErrUnknownBalancerElement returned when requested on a unknown Balancer element.
	ErrUnknownBalancerElement = errors.New("unknown balancer element")
	// ErrUnknownTypeOfBalancerElement returned when requested on a unknown types of Balancer element.
	ErrUnknownTypeOfBalancerElement = errors.New("unknown types of balancer element")
)

type cluster struct {
	config   config.Config
	pool     conn.PoolGetterCloser
	dial     func(context.Context, string) (*grpc.ClientConn, error)
	balancer balancer.Balancer
	explorer repeater.Repeater

	index     map[string]entry.Entry
	endpoints map[uint32]conn.Conn // only one endpoint by node ID

	mu     sync.RWMutex
	closed bool
}

func (c *cluster) GetConn(endpoint endpoint.Endpoint) conn.Conn {
	return c.pool.GetConn(endpoint)
}

func (c *cluster) Force() {
	c.explorer.Force()
}

func (c *cluster) SetExplorer(repeater repeater.Repeater) {
	c.explorer = repeater
}

type CRUD interface {
	Insert(ctx context.Context, endpoint endpoint.Endpoint) conn.Conn
	Update(ctx context.Context, endpoint endpoint.Endpoint) conn.Conn
	Remove(ctx context.Context, endpoint endpoint.Endpoint) conn.Conn
	Get(ctx context.Context) (cc conn.Conn, err error)
}

type Pessimizer interface {
	Pessimize(ctx context.Context, endpoint endpoint.Endpoint) error
}

type Explorer interface {
	SetExplorer(repeater repeater.Repeater)
	Force()
}

type CRUDExplorer interface {
	CRUD
	Explorer
}

type Cluster interface {
	closer.Closer
	CRUD
	Pessimizer
	Explorer
	conn.PoolGetter
}

func New(
	ctx context.Context,
	config config.Config,
	balancer balancer.Balancer,
) Cluster {
	return &cluster{
		config:    config,
		pool:      conn.NewPool(ctx, config),
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
		balancer:  balancer,
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
	c.endpoints = nil

	c.mu.Unlock()

	for _, entry := range index {
		if entry.Conn != nil {
			_ = entry.Conn.Close(ctx)
		}
	}

	return c.pool.Close(ctx)
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (cc conn.Conn, err error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, ErrClusterClosed
	}

	onDone := trace.DriverOnClusterGet(c.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(cc.Endpoint(), nil)
		}
	}()

	if e, ok := ContextEndpoint(ctx); ok {
		cc, ok = c.endpoints[e.NodeID()]
		if ok && cc.IsState(
			conn.Created,
			conn.Online,
			conn.Offline,
		) {
			return cc, nil
		}
	}

	cc = c.balancer.Next()
	if cc == nil {
		return nil, ErrClusterEmpty
	}

	return cc, nil
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e endpoint.Endpoint) (cc conn.Conn) {
	onDone := trace.DriverOnClusterInsert(c.config.Trace(), &ctx, e)
	defer func() {
		if cc != nil {
			onDone(cc.GetState())
		} else {
			onDone(conn.Unknown)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	cc = c.pool.GetConn(e)

	_, has := c.index[e.Address()]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}

	var wait chan struct{}
	defer func() {
		if wait != nil {
			close(wait)
		}
	}()

	entry := entry.Entry{Conn: cc}
	entry.InsertInto(c.balancer)
	c.index[e.Address()] = entry
	if e.NodeID() > 0 {
		c.endpoints[e.NodeID()] = cc
	}

	return cc
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(ctx context.Context, e endpoint.Endpoint) (cc conn.Conn) {
	onDone := trace.DriverOnClusterUpdate(c.config.Trace(), &ctx, e)
	defer func() {
		if cc != nil {
			onDone(cc.GetState())
		} else {
			onDone(conn.Unknown)
		}
	}()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	entry, has := c.index[e.Address()]
	if !has {
		panic("ydb: can't update not-existing endpoint")
	}
	if entry.Conn == nil {
		panic("ydb: cluster entry with nil conn")
	}

	delete(c.endpoints, e.NodeID())
	c.index[e.Address()] = entry
	if e.NodeID() > 0 {
		c.endpoints[e.NodeID()] = entry.Conn
	}
	if entry.Handle != nil {
		// entry.Handle may be nil when connection is being tracked.
		c.balancer.Update(entry.Handle, info.Info{})
	}

	return entry.Conn
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(ctx context.Context, e endpoint.Endpoint) (cc conn.Conn) {
	onDone := trace.DriverOnClusterRemove(c.config.Trace(), &ctx, e)
	defer func() {
		if cc != nil {
			onDone(cc.GetState())
		} else {
			onDone(conn.Unknown)
		}
	}()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}

	entry, has := c.index[e.Address()]
	if !has {
		c.mu.Unlock()
		panic("ydb: can't remove not-existing endpoint")
	}

	entry.RemoveFrom(c.balancer)
	delete(c.index, e.Address())
	delete(c.endpoints, e.NodeID())

	c.mu.Unlock()

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close(ctx)
	}

	return entry.Conn
}

func (c *cluster) Pessimize(ctx context.Context, e endpoint.Endpoint) (err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return errors.Errorf(0, "cluster: pessimize failed: %w", ErrClusterClosed)
	}

	entry, has := c.index[e.Address()]
	if !has {
		return errors.Errorf(0, "cluster: pessimize failed: %w", ErrUnknownEndpoint)
	}
	if entry.Handle == nil {
		return errors.Errorf(0, "cluster: pessimize failed: %w", ErrNilBalancerElement)
	}
	if !c.balancer.Contains(entry.Handle) {
		return errors.Errorf(0, "cluster: pessimize failed: %w", ErrUnknownBalancerElement)
	}
	entry.Conn.SetState(ctx, conn.Banned)
	if c.explorer != nil {
		// count ratio (banned/all)
		online := 0
		for _, entry = range c.index {
			if entry.Conn != nil && entry.Conn.GetState() == conn.Online {
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
	return strings.Compare(a.Address(), b.Address())
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
