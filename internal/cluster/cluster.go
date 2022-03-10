package cluster

import (
	"context"
	"fmt"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = fmt.Errorf("cluster closed")

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = fmt.Errorf("cluster empty")
)

type cluster struct {
	config config.Config
	pool   conn.Pool
	dial   func(context.Context, string) (*grpc.ClientConn, error)

	balancerMtx sync.RWMutex
	balancer    balancer.Balancer

	explorer repeater.Repeater

	index     map[string]entry.Entry
	endpoints map[uint32]conn.Conn // only one endpoint by node ID

	mu     sync.RWMutex
	closed bool
}

func (c *cluster) Pessimize(ctx context.Context, cc conn.Conn, cause error) {
	c.pool.Pessimize(ctx, cc, cause)

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return
	}

	entry, has := c.index[cc.Endpoint().Address()]
	if !has {
		return
	}

	if entry.Handle == nil {
		return
	}

	c.balancerMtx.Lock()
	defer c.balancerMtx.Unlock()

	if !c.balancer.Contains(entry.Handle) {
		return
	}

	if c.explorer == nil {
		return
	}

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

func (c *cluster) Lock() {
	c.mu.Lock()
}

func (c *cluster) Unlock() {
	c.mu.Unlock()
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

type crudOptionsHolder struct {
	withLock bool
}

type crudOption func(h *crudOptionsHolder)

func WithoutLock() crudOption {
	return func(h *crudOptionsHolder) {
		h.withLock = false
	}
}

func parseOptions(opts ...crudOption) *crudOptionsHolder {
	h := &crudOptionsHolder{
		withLock: true,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

type CRUD interface {
	// Insert inserts endpoint to cluster
	Insert(ctx context.Context, endpoint endpoint.Endpoint, opts ...crudOption) conn.Conn

	// Update updates endpoint in cluster
	Update(ctx context.Context, endpoint endpoint.Endpoint, opts ...crudOption) conn.Conn

	// Remove removes endpoint from cluster
	Remove(ctx context.Context, endpoint endpoint.Endpoint, opts ...crudOption) conn.Conn

	// Get gets conn from cluster
	Get(ctx context.Context, opts ...crudOption) (cc conn.Conn, err error)
}

type Explorer interface {
	SetExplorer(repeater repeater.Repeater)
	Force()
}

type Locker interface {
	Lock()
	Unlock()
}

type CRUDExplorerLocker interface {
	CRUD
	Explorer
	Locker
}

type Cluster interface {
	closer.Closer
	CRUD
	Explorer
	Locker
	conn.PoolGetter
	conn.Pessimizer
}

func New(
	ctx context.Context,
	config config.Config,
	pool conn.Pool,
	balancer balancer.Balancer,
) Cluster {
	onDone := trace.DriverOnClusterInit(config.Trace(), &ctx)
	defer func() {
		onDone(pool.Take(ctx))
	}()

	return &cluster{
		config:    config,
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
		pool:      pool,
		balancer:  balancer,
	}
}

func (c *cluster) Close(ctx context.Context) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	if c.explorer != nil {
		c.explorer.Stop()
	}
	c.closed = true

	c.index = nil
	c.endpoints = nil

	return c.pool.Release(ctx)
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context, opts ...crudOption) (cc conn.Conn, err error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.Error(ErrClusterClosed)
	}

	onDone := trace.DriverOnClusterGet(c.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(cc.Endpoint().Copy(), nil)
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

	c.balancerMtx.RLock()
	defer c.balancerMtx.RUnlock()

	cc = c.balancer.Next()
	if cc == nil {
		return nil, errors.Error(ErrClusterEmpty)
	}

	return cc, nil
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e endpoint.Endpoint, opts ...crudOption) (cc conn.Conn) {
	var (
		onDone   = trace.DriverOnClusterInsert(c.config.Trace(), &ctx, e.Copy())
		inserted = false
	)
	defer func() {
		onDone(inserted, cc.GetState())
	}()

	options := parseOptions(opts...)
	if options.withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if c.closed {
		return nil
	}

	cc = c.pool.GetConn(e)

	_, has := c.index[e.Address()]
	if has {
		panic("ydb: can't insert already existing endpoint")
	}

	cc.Endpoint().Touch()

	entry := entry.Entry{Conn: cc}

	inserted = entry.InsertInto(c.balancer, &c.balancerMtx)

	c.index[e.Address()] = entry

	if e.NodeID() > 0 {
		c.endpoints[e.NodeID()] = cc
	}

	return cc
}

// Update updates existing connection's runtime stats such that load factor and others.
func (c *cluster) Update(ctx context.Context, e endpoint.Endpoint, opts ...crudOption) (cc conn.Conn) {
	onDone := trace.DriverOnClusterUpdate(c.config.Trace(), &ctx, e.Copy())
	defer func() {
		if cc != nil {
			onDone(cc.GetState())
		} else {
			onDone(conn.Unknown)
		}
	}()

	options := parseOptions(opts...)
	if options.withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

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

	entry.Conn.Endpoint().Touch()

	delete(c.endpoints, e.NodeID())
	c.index[e.Address()] = entry

	if e.NodeID() > 0 {
		c.endpoints[e.NodeID()] = entry.Conn
	}

	c.balancerMtx.Lock()
	defer c.balancerMtx.Unlock()

	c.balancer.Update(entry.Handle, e.Info())

	return entry.Conn
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(ctx context.Context, e endpoint.Endpoint, opts ...crudOption) (cc conn.Conn) {
	var (
		onDone  = trace.DriverOnClusterRemove(c.config.Trace(), &ctx, e.Copy())
		removed = false
	)
	defer func() {
		onDone(cc.GetState(), removed)
	}()

	options := parseOptions(opts...)
	if options.withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if c.closed {
		return
	}

	entry, has := c.index[e.Address()]
	if !has {
		panic("ydb: can't remove not-existing endpoint")
	}

	removed = entry.RemoveFrom(c.balancer, &c.balancerMtx)

	delete(c.index, e.Address())
	delete(c.endpoints, e.NodeID())

	if entry.Conn != nil {
		// entry.Conn may be nil when connection is being tracked after unsuccessful dial().
		_ = entry.Conn.Close(ctx)
	}

	return entry.Conn
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
