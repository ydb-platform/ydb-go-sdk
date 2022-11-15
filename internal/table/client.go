package table

import (
	"container/list"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type sessionBuilderOption func(s *session)

// sessionBuilder is the interface that holds logic of creating sessions.
type sessionBuilder func(ctx context.Context, opts ...sessionBuilderOption) (*session, error)

type balancerNotifier interface {
	grpc.ClientConnInterface

	OnUpdate(onDiscovery func(ctx context.Context, endpoints []endpoint.Info))
}

func New(balancer balancerNotifier, config config.Config) *Client {
	return newClient(balancer, func(ctx context.Context, opts ...sessionBuilderOption) (s *session, err error) {
		return newSession(ctx, balancer, config, opts...)
	}, config)
}

func newClient(
	balancer balancerNotifier,
	builder sessionBuilder,
	config config.Config,
) *Client {
	var (
		ctx    = context.Background()
		onDone = trace.TableOnInit(config.Trace(), &ctx)
	)
	c := &Client{
		config: config,
		cc:     balancer,
		build:  builder,
		index:  make(map[*session]sessionInfo),
		nodes:  make(map[uint32]map[*session]struct{}),
		idle:   list.New(),
		waitq:  list.New(),
		limit:  config.SizeLimit(),
		waitChPool: sync.Pool{
			New: func() interface{} {
				ch := make(chan *session)
				return &ch
			},
		},
		done: make(chan struct{}),
	}
	if balancer != nil {
		balancer.OnUpdate(c.updateNodes)
	}
	if idleThreshold := config.IdleThreshold(); idleThreshold > 0 {
		c.wg.Add(1)
		go c.internalPoolGC(ctx, idleThreshold)
	}
	onDone(c.limit)
	return c
}

// Client is a set of session instances that may be reused.
// A Client is safe for use by multiple goroutines simultaneously.
type Client struct {
	// read-only fields
	config config.Config
	build  sessionBuilder
	cc     grpc.ClientConnInterface

	// read-write fields
	mu                xsync.Mutex
	index             map[*session]sessionInfo
	nodes             map[uint32]map[*session]struct{}
	createInProgress  int        // KIKIMR-9163: in-create-process counter
	limit             int        // Upper bound for Client size.
	idle              *list.List // list<*session>
	waitq             *list.List // list<*chan *session>
	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
	wg                sync.WaitGroup
	done              chan struct{}
}

type createSessionOptions struct {
	onCreate []func(s *session)
	onClose  []func(s *session)
}

type createSessionOption func(o *createSessionOptions)

func withCreateSessionOnCreate(onCreate func(s *session)) createSessionOption {
	return func(o *createSessionOptions) {
		o.onCreate = append(o.onCreate, onCreate)
	}
}

func withCreateSessionOnClose(onClose func(s *session)) createSessionOption {
	return func(o *createSessionOptions) {
		o.onClose = append(o.onClose, onClose)
	}
}

func (c *Client) updateNodes(ctx context.Context, endpoints []endpoint.Info) {
	nodeIDs := make([]uint32, len(endpoints))
	for i, e := range endpoints {
		nodeIDs[i] = e.NodeID()
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})
	c.mu.WithLock(func() {
		if c.isClosed() {
			return
		}
		for nodeID := range c.nodes {
			if sort.Search(len(nodeIDs), func(i int) bool {
				return nodeIDs[i] >= nodeID
			}) == len(nodeIDs) {
				for s := range c.nodes[nodeID] {
					func(s *session) {
						if info, has := c.index[s]; has && info.idle != nil {
							s.SetStatus(table.SessionClosing)
							c.wg.Add(1)
							go func() {
								defer c.wg.Done()
								c.internalPoolSyncCloseSession(ctx, s)
							}()
						} else {
							s.SetStatus(table.SessionClosing)
						}
					}(s)
				}
			}
		}
	})
}

func (c *Client) createSession(ctx context.Context, opts ...createSessionOption) (s *session, err error) {
	options := createSessionOptions{}
	for _, o := range opts {
		o(&options)
	}

	defer func() {
		if s == nil {
			return
		}
		for _, onCreate := range options.onCreate {
			onCreate(s)
		}
		s.onClose = append(s.onClose, options.onClose...)
	}()

	type result struct {
		s   *session
		err error
	}

	ch := make(chan result)

	select {
	case <-c.done:
		return nil, xerrors.WithStackTrace(errClosedClient)

	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())

	default:
		c.mu.WithLock(func() {
			if c.isClosed() {
				return
			}
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()

				var (
					s   *session
					err error
				)

				createSessionCtx := xcontext.WithoutDeadline(ctx)

				if timeout := c.config.CreateSessionTimeout(); timeout > 0 {
					var cancel context.CancelFunc
					createSessionCtx, cancel = context.WithTimeout(createSessionCtx, timeout)
					defer cancel()
				}

				s, err = c.build(createSessionCtx)

				closeSession := func(s *session) {
					if s == nil {
						return
					}

					closeSessionCtx := xcontext.WithoutDeadline(ctx)

					if timeout := c.config.DeleteTimeout(); timeout > 0 {
						var cancel context.CancelFunc
						createSessionCtx, cancel = context.WithTimeout(closeSessionCtx, timeout)
						defer cancel()
					}

					_ = s.Close(ctx)
				}

				select {
				case ch <- result{
					s:   s,
					err: err,
				}: // nop

				case <-c.done:
					closeSession(s)

				case <-ctx.Done():
					closeSession(s)
				}
			}()
		})
	}

	select {
	case <-c.done:
		return nil, xerrors.WithStackTrace(errClosedClient)

	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())

	case r := <-ch:
		if r.err != nil {
			return nil, xerrors.WithStackTrace(r.err)
		}
		return r.s, nil
	}
}

func (c *Client) appendSessionToNodes(s *session) {
	c.mu.WithLock(func() {
		nodeID := s.NodeID()
		sessions, has := c.nodes[nodeID]
		if !has {
			sessions = make(map[*session]struct{})
		}
		sessions[s] = struct{}{}
		c.nodes[nodeID] = sessions
	})
}

func (c *Client) removeSessionFromNodes(s *session) {
	c.mu.WithLock(func() {
		nodeID := s.NodeID()
		sessions, has := c.nodes[nodeID]
		if !has {
			sessions = make(map[*session]struct{})
		}
		delete(sessions, s)
		if len(sessions) == 0 {
			delete(c.nodes, nodeID)
		} else {
			c.nodes[nodeID] = sessions
		}
	})
}

func (c *Client) CreateSession(ctx context.Context, opts ...table.Option) (_ table.ClosableSession, err error) {
	if c == nil {
		return nil, xerrors.WithStackTrace(errNilClient)
	}
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedClient)
	}
	var s *session
	createSession := func(ctx context.Context) (*session, error) {
		s, err = c.createSession(ctx,
			withCreateSessionOnCreate(c.appendSessionToNodes),
			withCreateSessionOnClose(c.removeSessionFromNodes),
		)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		return s, nil
	}
	if !c.config.AutoRetry() {
		s, err = createSession(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		return s, nil
	}
	options := retryOptions(c.config.Trace(), opts...)
	err = retry.Retry(
		ctx,
		func(ctx context.Context) (err error) {
			s, err = createSession(ctx)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}
			return nil
		},
		retry.WithIdempotent(true),
		retry.WithID("CreateSession"),
		retry.WithFastBackoff(options.FastBackoff),
		retry.WithSlowBackoff(options.SlowBackoff),
		retry.WithTrace(trace.Retry{
			OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				onIntermediate := trace.TableOnCreateSession(c.config.Trace(), info.Context)
				return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
					onDone := onIntermediate(info.Error)
					return func(info trace.RetryLoopDoneInfo) {
						onDone(s, info.Attempts, info.Error)
					}
				}
			},
		}),
	)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	return s, nil
}

func (c *Client) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// c.mu must NOT be held.
func (c *Client) internalPoolCreateSession(ctx context.Context) (s *session, err error) {
	if c.isClosed() {
		return nil, errClosedClient
	}
	// pre-check the Client size
	var enoughSpace bool
	c.mu.WithLock(func() {
		enoughSpace = c.createInProgress+len(c.index) < c.limit
		if enoughSpace {
			c.createInProgress++
		}
	})

	if !enoughSpace {
		return nil, xerrors.WithStackTrace(errSessionPoolOverflow)
	}

	defer func() {
		c.mu.WithLock(func() {
			c.createInProgress--
		})
	}()

	s, err = c.createSession(
		meta.WithAllowFeatures(ctx,
			meta.HintSessionBalancer,
		),
		withCreateSessionOnCreate(c.appendSessionToNodes),
		withCreateSessionOnClose(c.removeSessionFromNodes),
		withCreateSessionOnCreate(func(s *session) {
			c.mu.WithLock(func() {
				c.index[s] = sessionInfo{
					touched: timeutil.Now(),
				}
				trace.TableOnPoolSessionAdd(c.config.Trace(), s)
				trace.TableOnPoolStateChange(c.config.Trace(), len(c.index), "append")
			})
		}), withCreateSessionOnClose(func(s *session) {
			c.mu.WithLock(func() {
				info, has := c.index[s]
				if !has {
					panic("session not found in pool")
				}

				delete(c.index, s)

				trace.TableOnPoolSessionRemove(c.config.Trace(), s)
				trace.TableOnPoolStateChange(c.config.Trace(), len(c.index), "remove")

				if !c.isClosed() {
					c.internalPoolNotify(nil)
				}

				if info.idle != nil {
					c.idle.Remove(info.idle)
				}
			})
		}))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return s, nil
}

type getOptions struct {
	t trace.Table
}

type getOption func(o *getOptions)

func withTrace(t trace.Table) getOption {
	return func(o *getOptions) {
		o.t = o.t.Compose(t)
	}
}

func (c *Client) internalPoolGet(ctx context.Context, opts ...getOption) (s *session, err error) {
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedClient)
	}

	var (
		start = time.Now()
		i     = 0
		o     = getOptions{t: c.config.Trace()}
	)
	for _, opt := range opts {
		opt(&o)
	}

	onDone := trace.TableOnPoolGet(o.t, &ctx)
	defer func() {
		onDone(s, i, err)
	}()

	const maxAttempts = 100
	for s == nil && err == nil && i < maxAttempts && !c.isClosed() {
		i++
		// First, we try to internalPoolGet session from idle
		c.mu.WithLock(func() {
			s = c.internalPoolRemoveFirstIdle()
		})

		if s != nil {
			return s, nil
		}

		// Second, we try to create new session
		s, err = c.internalPoolCreateSession(ctx)
		if s == nil && err == nil {
			if err = ctx.Err(); err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			panic("both of session and err are nil")
		}
		// got session or err is not recoverable
		if s != nil || !isCreateSessionErrorRetriable(err) {
			return s, xerrors.WithStackTrace(err)
		}

		// Third, we try to wait for a touched session - Client is full.
		//
		// This should be done only if number of currently waiting goroutines
		// are less than maximum amount of touched session. That is, we want to
		// be fair here and not to lock more goroutines than we could ship
		// session to.
		s, err = c.internalPoolWaitFromCh(ctx, o.t)
		if err != nil {
			err = xerrors.WithStackTrace(err)
		}
	}
	if s == nil && err == nil {
		if c.isClosed() {
			err = xerrors.WithStackTrace(errClosedClient)
		} else {
			err = xerrors.WithStackTrace(errNoProgress)
		}
	}
	if err != nil {
		var (
			index            int
			idle             int
			createInProgress int
		)
		c.mu.WithLock(func() {
			index = len(c.index)
			idle = c.idle.Len()
			createInProgress = c.createInProgress
		})
		return s, xerrors.WithStackTrace(
			fmt.Errorf("failed to get session from pool ("+
				"attempts: %d, latency: %v, stats: {index: %d, idle: %d, create_in_progress: %d}"+
				"): %w", i, time.Since(start), index, idle, createInProgress, err,
			),
		)
	}
	return s, nil
}

// Get returns first idle session from the Client and removes it from
// there. If no items stored in Client it creates new one returns it.
func (c *Client) Get(ctx context.Context) (s *session, err error) {
	return c.internalPoolGet(ctx)
}

func (c *Client) internalPoolWaitFromCh(ctx context.Context, t trace.Table) (s *session, err error) {
	var (
		ch *chan *session
		el *list.Element // Element in the wait queue.
		ok bool
	)

	c.mu.WithLock(func() {
		ch = c.internalPoolGetWaitCh()
		el = c.waitq.PushBack(ch)
	})

	waitDone := trace.TableOnPoolWait(t, &ctx)

	defer func() {
		waitDone(s, err)
	}()

	select {
	case <-c.done:
		c.mu.WithLock(func() {
			c.waitq.Remove(el)
		})
		return nil, xerrors.WithStackTrace(errClosedClient)

	case s, ok = <-*ch:
		// Note that race may occur and some goroutine may try to write
		// session into channel after it was enqueued but before it being
		// read here. In that case we will receive nil here and will retry.
		//
		// The same way will work when some session become deleted - the
		// nil value will be sent into the channel.
		if ok {
			// Put only filled and not closed channel back to the Client.
			// That is, we need to avoid races on filling reused channel
			// for the next waiter – session could be lost for a long time.
			c.internalPoolPutWaitCh(ch)
		}
		return s, nil

	case <-time.After(c.config.CreateSessionTimeout()):
		c.mu.WithLock(func() {
			c.waitq.Remove(el)
		})
		return nil, nil

	case <-ctx.Done():
		c.mu.WithLock(func() {
			c.waitq.Remove(el)
		})
		return nil, xerrors.WithStackTrace(ctx.Err())
	}
}

// Put returns session to the Client for further reuse.
// If Client is already closed Put() calls s.Close(ctx) and returns
// errClosedClient.
// If Client is overflow calls s.Close(ctx) and returns
// errSessionPoolOverflow.
//
// Note that Put() must be called only once after being created or received by
// Get() or Take() calls. In other way it will produce unexpected behavior or
// panic.
func (c *Client) Put(ctx context.Context, s *session) (err error) {
	onDone := trace.TableOnPoolPut(c.config.Trace(), &ctx, s)
	defer func() {
		onDone(err)
	}()

	defer func() {
		if err != nil {
			c.internalPoolSyncCloseSession(ctx, s)
		}
	}()

	switch {
	case c.isClosed():
		return xerrors.WithStackTrace(errClosedClient)

	case s.isClosing():
		return xerrors.WithStackTrace(errSessionUnderShutdown)

	case s.isClosed():
		return xerrors.WithStackTrace(errSessionClosed)

	default:
		c.mu.Lock()
		defer c.mu.Unlock()

		if c.idle.Len() >= c.limit {
			return xerrors.WithStackTrace(errSessionPoolOverflow)
		}

		if !c.internalPoolNotify(s) {
			c.internalPoolPushIdle(s, timeutil.Now())
		}

		return nil
	}
}

// Close deletes all stored sessions inside Client.
// It also stops all underlying timers and goroutines.
// It returns first error occurred during stale sessions' deletion.
// Note that even on error it calls Close() on each session.
func (c *Client) Close(ctx context.Context) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	c.mu.WithLock(func() {
		select {
		case <-c.done:
			return

		default:
			close(c.done)

			onDone := trace.TableOnClose(c.config.Trace(), &ctx)
			defer func() {
				onDone(err)
			}()

			c.limit = 0

			for el := c.waitq.Front(); el != nil; el = el.Next() {
				ch := el.Value.(*chan *session)
				close(*ch)
			}

			for e := c.idle.Front(); e != nil; e = e.Next() {
				s := e.Value.(*session)
				s.SetStatus(table.SessionClosing)
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					c.internalPoolSyncCloseSession(ctx, s)
				}()
			}
		}
	})

	c.wg.Wait()

	return nil
}

// Do provide the best effort for execute operation
// Do implements internal busy loop until one of the following conditions is met:
// - deadline was canceled or deadlined
// - retry operation returned nil as error
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
func (c *Client) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if c.isClosed() {
		return xerrors.WithStackTrace(errClosedClient)
	}
	opts = append(opts, table.WithTrace(c.config.Trace()))
	return do(
		ctx,
		c,
		c.config,
		op,
		retryOptions(c.config.Trace(), opts...),
	)
}

func (c *Client) DoTx(ctx context.Context, op table.TxOperation, opts ...table.Option) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	if c.isClosed() {
		return xerrors.WithStackTrace(errClosedClient)
	}
	err = doTx(
		ctx,
		c,
		c.config,
		op,
		retryOptions(c.config.Trace(), opts...),
	)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

func (c *Client) internalPoolGCTick(ctx context.Context, idleThreshold time.Duration) {
	c.mu.WithLock(func() {
		if c.isClosed() {
			return
		}
		for e := c.idle.Front(); e != nil; e = e.Next() {
			s := e.Value.(*session)
			info, has := c.index[s]
			if !has {
				panic("session not found in pool")
			}
			if info.idle == nil {
				panic("inconsistent session info")
			}
			if since := timeutil.Until(info.touched); since > idleThreshold {
				s.SetStatus(table.SessionClosing)
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					c.internalPoolSyncCloseSession(ctx, s)
				}()
			}
		}
	})
}

func (c *Client) internalPoolGC(ctx context.Context, idleThreshold time.Duration) {
	defer c.wg.Done()

	timer := timeutil.NewTimer(idleThreshold)
	defer timer.Stop()

	for {
		select {
		case <-c.done:
			return

		case <-ctx.Done():
			return

		case <-timer.C():
			c.internalPoolGCTick(ctx, idleThreshold)
			timer.Reset(idleThreshold / 2)
		}
	}
}

// internalPoolGetWaitCh returns pointer to a channel of sessions.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Client.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (c *Client) internalPoolGetWaitCh() *chan *session {
	if c.testHookGetWaitCh != nil {
		c.testHookGetWaitCh()
	}
	ch := c.waitChPool.Get()
	s, ok := ch.(*chan *session)
	if !ok {
		panic(fmt.Sprintf("%T is not a chan of sessions", ch))
	}
	return s
}

// internalPoolPutWaitCh receives pointer to a channel and makes it available for further
// use.
// Note that ch MUST NOT be owned by any goroutine at the call moment and ch
// MUST NOT contain any value.
func (c *Client) internalPoolPutWaitCh(ch *chan *session) {
	c.waitChPool.Put(ch)
}

// c.mu must be held.
func (c *Client) internalPoolPeekFirstIdle() (s *session, touched time.Time) {
	el := c.idle.Front()
	if el == nil {
		return
	}
	s = el.Value.(*session)
	info, has := c.index[s]
	if !has || el != info.idle {
		panic("inconsistent session client index")
	}
	return s, info.touched
}

// removes first session from idle and resets the keepAliveCount
// to prevent session from dying in the internalPoolGC after it was returned
// to be used only in outgoing functions that make session busy.
// c.mu must be held.
func (c *Client) internalPoolRemoveFirstIdle() *session {
	s, _ := c.internalPoolPeekFirstIdle()
	if s != nil {
		info := c.internalPoolRemoveIdle(s)
		c.index[s] = info
	}
	return s
}

// c.mu must be held.
func (c *Client) internalPoolNotify(s *session) (notified bool) {
	for el := c.waitq.Front(); el != nil; el = c.waitq.Front() {
		// Some goroutine is waiting for a session.
		//
		// It could be in this states:
		//   1) Reached the select code and awaiting for a value in channel.
		//   2) Reached the select code but already in branch of deadline
		//   cancellation. In this case it is locked on c.mu.Lock().
		//   3) Not reached the select code and thus not reading yet from the
		//   channel.
		//
		// For cases (2) and (3) we close the channel to signal that goroutine
		// missed something and may want to retry (especially for case (3)).
		//
		// After that we taking a next waiter and repeat the same.
		ch := c.waitq.Remove(el).(*chan *session)
		select {
		case *ch <- s:
			// Case (1).
			return true

		case <-c.done:
			// Case (2) or (3).
			close(*ch)

		default:
			// Case (2) or (3).
			close(*ch)
		}
	}
	return false
}

func (c *Client) internalPoolSyncCloseSession(ctx context.Context, s *session) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, c.config.DeleteTimeout())
	defer cancel()

	_ = s.Close(ctx)
}

// c.mu must be held.
func (c *Client) internalPoolRemoveIdle(s *session) sessionInfo {
	info, has := c.index[s]
	if !has || info.idle == nil {
		panic("inconsistent session client index")
	}

	c.idle.Remove(info.idle)
	info.idle = nil
	c.index[s] = info
	return info
}

// c.mu must be held.
func (c *Client) internalPoolPushIdle(s *session, now time.Time) {
	c.internalPoolHandlePushIdle(s, now, c.idle.PushBack(s))
}

// c.mu must be held.
func (c *Client) internalPoolHandlePushIdle(s *session, now time.Time, el *list.Element) {
	info, has := c.index[s]
	if !has {
		panic("trying to store session created outside of the client")
	}
	if info.idle != nil {
		panic("inconsistent session client index")
	}

	info.touched = now
	info.idle = el
	c.index[s] = info
}

type sessionInfo struct {
	idle    *list.Element
	touched time.Time
}
