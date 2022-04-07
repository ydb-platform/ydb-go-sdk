package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = xerrors.Wrap(fmt.Errorf("cluster closed"))

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))
)

type cluster struct {
	config config.Config
	pool   conn.Pool

	mu        sync.RWMutex
	explorer  repeater.Repeater
	index     map[string]entry.Entry
	endpoints map[uint32]conn.Conn // only one endpoint by node ID

	done chan struct{}
}

func (c *cluster) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *cluster) Pessimize(ctx context.Context, cc conn.Conn, cause error) {
	c.pool.Pessimize(ctx, cc, cause)

	if c.isClosed() {
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, has := c.index[cc.Endpoint().Address()]
	if !has {
		return
	}

	if entry.Handle == nil {
		return
	}

	if !c.config.Balancer().Contains(entry.Handle) {
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

type Getter interface {
	// Get gets conn from cluster
	Get(ctx context.Context) (cc conn.Conn, err error)
}

type Inserter interface {
	// Insert inserts endpoint to cluster
	Insert(ctx context.Context, endpoint endpoint.Endpoint, opts ...crudOption)
}

type Remover interface {
	// Remove removes endpoint from cluster
	Remove(ctx context.Context, endpoint endpoint.Endpoint, opts ...crudOption)
}

type Explorer interface {
	SetExplorer(repeater repeater.Repeater)
	Force()
}

type InserterRemoverExplorerLocker interface {
	Inserter
	Remover
	Explorer
	sync.Locker
}

type Cluster interface {
	closer.Closer
	Getter
	Inserter
	Remover
	Explorer
	sync.Locker
	conn.Pessimizer
}

func New(
	ctx context.Context,
	config config.Config,
	pool conn.Pool,
) Cluster {
	onDone := trace.DriverOnClusterInit(config.Trace(), &ctx)
	defer func() {
		onDone(pool.Take(ctx))
	}()

	return &cluster{
		done:      make(chan struct{}),
		config:    config,
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
		pool:      pool,
	}
}

func (c *cluster) Close(ctx context.Context) (err error) {
	defer close(c.done)

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.explorer != nil {
		c.explorer.Stop()
	}

	for _, entry := range c.index {
		c.Remove(
			ctx,
			entry.Conn.Endpoint(),
			WithoutLock(),
		)
	}

	var issues []error
	if len(c.index) > 0 {
		issues = append(issues, fmt.Errorf(
			"non empty index after remove all entries: %v",
			func() (endpoints []string) {
				for e := range c.index {
					endpoints = append(endpoints, e)
				}
				return endpoints
			}(),
		))
	}

	if len(c.endpoints) > 0 {
		issues = append(issues, fmt.Errorf(
			"non empty nodes after remove all entries: %v",
			func() (nodes []uint32) {
				for e := range c.endpoints {
					nodes = append(nodes, e)
				}
				return nodes
			}(),
		))
	}

	if err = c.pool.Release(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("cluster closed with issues", issues...))
	}

	return nil
}

func (c *cluster) get(ctx context.Context) (cc conn.Conn, err error) {
	for {
		select {
		case <-c.done:
			return nil, xerrors.WithStackTrace(ErrClusterClosed)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			cc = c.config.Balancer().Next()
			if cc == nil {
				return nil, xerrors.WithStackTrace(ErrClusterEmpty)
			}
			if err = cc.Ping(ctx); err == nil {
				return cc, nil
			}
		}
	}
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *cluster) Get(ctx context.Context) (cc conn.Conn, err error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	if c.isClosed() {
		return nil, xerrors.WithStackTrace(ErrClusterClosed)
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
		c.mu.RLock()
		cc, ok = c.endpoints[e.NodeID()]
		c.mu.RUnlock()
		if ok && cc.IsState(
			conn.Created,
			conn.Online,
			conn.Offline,
		) {
			if err = cc.Ping(ctx); err == nil {
				return cc, nil
			}
		}
	}

	return c.get(ctx)
}

// Insert inserts new connection into the cluster.
func (c *cluster) Insert(ctx context.Context, e endpoint.Endpoint, opts ...crudOption) {
	var (
		onDone   = trace.DriverOnClusterInsert(c.config.Trace(), &ctx, e.Copy())
		inserted = false
		state    conn.State
	)
	defer func() {
		onDone(inserted, state)
	}()

	options := parseOptions(opts...)
	if options.withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if c.isClosed() {
		return
	}

	cc := c.pool.Get(e)

	cc.Endpoint().Touch()

	entry := entry.Entry{
		Conn:   cc,
		Handle: c.config.Balancer().Insert(cc),
	}

	inserted = entry.Handle != nil

	c.index[e.Address()] = entry

	if e.NodeID() > 0 {
		c.endpoints[e.NodeID()] = cc
	}

	state = cc.GetState()
}

// Remove removes and closes previously inserted connection.
func (c *cluster) Remove(ctx context.Context, e endpoint.Endpoint, opts ...crudOption) {
	var (
		onDone  = trace.DriverOnClusterRemove(c.config.Trace(), &ctx, e.Copy())
		address = e.Address()
		nodeID  = e.NodeID()
		removed bool
		state   conn.State
	)
	defer func() {
		onDone(removed, state)
	}()

	options := parseOptions(opts...)
	if options.withLock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if c.isClosed() {
		return
	}

	entry, has := c.index[address]
	if !has {
		panic("ydb: can't remove not-existing endpoint")
	}

	defer func() {
		_ = entry.Conn.Release(ctx)
	}()

	if entry.Handle != nil {
		removed = c.config.Balancer().Remove(entry.Handle)
		entry.Handle = nil
	}

	delete(c.index, address)
	delete(c.endpoints, nodeID)

	state = entry.Conn.GetState()
}

func compareEndpoints(a, b endpoint.Endpoint) int {
	return strings.Compare(
		a.Address(),
		b.Address(),
	)
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
		eq, add, del,
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
