package table

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"

	metaHeaders "github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// sessionBuilder is the interface that holds logic of creating sessions.
type sessionBuilder func(ctx context.Context) (*session, error)

func New(ctx context.Context, cc grpc.ClientConnInterface, config *config.Config) *Client {
	onDone := trace.TableOnInit(config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.New"),
	)
	defer func() {
		onDone(config.SizeLimit())
	}()

	return newClient(ctx, cc, func(ctx context.Context) (s *session, err error) {
		return newSession(ctx, cc, config)
	}, config)
}

func newClient(
	ctx context.Context,
	cc grpc.ClientConnInterface,
	builder sessionBuilder,
	config *config.Config,
) *Client {
	c := &Client{
		clock:  config.Clock(),
		config: config,
		cc:     cc,
		build:  builder,
		index:  make(map[*session]sessionInfo),
		idle:   xlist.New[*session](),
		waitQ:  xlist.New[*chan *session](),
		limit:  config.SizeLimit(),
		waitChPool: sync.Pool{
			New: func() interface{} {
				ch := make(chan *session)

				return &ch
			},
		},
		done: make(chan struct{}),
	}
	if idleThreshold := config.IdleThreshold(); idleThreshold > 0 {
		c.wg.Add(1)
		go c.internalPoolGC(ctx, idleThreshold)
	}

	return c
}

// Client is a set of session instances that may be reused.
// A Client is safe for use by multiple goroutines simultaneously.
type Client struct {
	// read-only fields
	config *config.Config
	build  sessionBuilder
	cc     grpc.ClientConnInterface
	clock  clockwork.Clock

	// read-write fields
	mu                xsync.Mutex
	index             map[*session]sessionInfo
	createInProgress  int // KIKIMR-9163: in-create-process counter
	limit             int // Upper bound for Client size.
	idle              *xlist.List[*session]
	waitQ             *xlist.List[*chan *session]
	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
	wg                sync.WaitGroup
	done              chan struct{}
}

type createSessionOptions struct {
	onCreate []func(s *session)
	onClose  []func(s *session)
}

type sessionResult struct {
	s   *session
	err error
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

func (c *Client) createSession(ctx context.Context, opts ...createSessionOption) (s *session, err error) {
	options := gatherOptions(opts)

	defer func() {
		if s != nil {
			applyOptions(s, options)
		}
	}()

	resultCh := make(chan sessionResult)
	if err := c.initiateSessionCreation(ctx, resultCh); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return c.waitForSessionCreation(ctx, resultCh)
}

func gatherOptions(opts []createSessionOption) createSessionOptions {
	options := createSessionOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	return options
}

func applyOptions(s *session, options createSessionOptions) {
	for _, onCreate := range options.onCreate {
		onCreate(s)
	}
	s.onClose = append(s.onClose, options.onClose...)
}

func (c *Client) initiateSessionCreation(ctx context.Context, resultCh chan<- sessionResult) error {
	select {
	case <-c.done:
		return errClosedClient

	case <-ctx.Done():
		return ctx.Err()

	default:
		c.mu.WithLock(func() {
			if c.isClosed() {
				return
			}
			c.wg.Add(1)
			go c.createSessionWorker(ctx, resultCh)
		})

		return nil
	}
}

func (c *Client) createSessionWorker(ctx context.Context, resultCh chan<- sessionResult) {
	defer c.wg.Done()

	createSessionCtx := xcontext.ValueOnly(ctx)
	if timeout := c.config.CreateSessionTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		createSessionCtx, cancel = xcontext.WithTimeout(createSessionCtx, timeout)
		defer cancel()
	}

	s, err := c.build(createSessionCtx)

	select {
	case resultCh <- sessionResult{s: s, err: err}:
	case <-c.done:
		c.closeSession(ctx, s)
	case <-ctx.Done():
		c.closeSession(ctx, s)
	}
}

func (c *Client) closeSession(ctx context.Context, s *session) {
	if s == nil {
		return
	}

	closeSessionCtx := xcontext.ValueOnly(ctx)
	if timeout := c.config.DeleteTimeout(); timeout > 0 {
		var cancel context.CancelFunc
		closeSessionCtx, cancel = xcontext.WithTimeout(closeSessionCtx, timeout)
		defer cancel()
	}

	_ = s.Close(closeSessionCtx)
}

func (c *Client) waitForSessionCreation(ctx context.Context, resultCh <-chan sessionResult) (*session, error) {
	select {
	case <-c.done:
		return nil, xerrors.WithStackTrace(errClosedClient)

	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())

	case r := <-resultCh:
		if r.err != nil {
			return nil, xerrors.WithStackTrace(r.err)
		}

		return r.s, nil
	}
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
		s, err = c.createSession(ctx)
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
	err = retry.Retry(ctx,
		func(ctx context.Context) (err error) {
			s, err = createSession(ctx)
			if err != nil {
				return xerrors.WithStackTrace(err)
			}

			return nil
		},
		append(
			[]retry.Option{
				retry.WithIdempotent(true),
				retry.WithTrace(&trace.Retry{
					OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
						onDone := trace.TableOnCreateSession(c.config.Trace(), info.Context,
							stack.FunctionID(
								"github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).CreateSession"))

						return func(info trace.RetryLoopDoneInfo) {
							onDone(s, info.Attempts, info.Error)
						}
					},
				}),
			}, c.retryOptions(opts...).RetryOptions...,
		)...,
	)

	return s, xerrors.WithStackTrace(err)
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
			metaHeaders.HintSessionBalancer,
		),
		withCreateSessionOnCreate(func(s *session) {
			c.mu.WithLock(func() {
				c.index[s] = sessionInfo{
					touched: c.clock.Now(),
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
	t *trace.Table
}

type getOption func(o *getOptions)

func withTrace(t *trace.Table) getOption {
	return func(o *getOptions) {
		o.t = o.t.Compose(t)
	}
}

func (c *Client) internalPoolGet(ctx context.Context, opts ...getOption) (s *session, err error) {
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedClient)
	}

	const maxAttempts = 100

	var (
		start = time.Now()
		i     = 0
		o     = getOptions{t: c.config.Trace()}
	)
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	onDone := trace.TableOnPoolGet(o.t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).internalPoolGet"),
	)
	defer func() {
		onDone(s, i, err)
	}()

	for s == nil && err == nil && i < maxAttempts && !c.isClosed() {
		i++
		s = tryGetIdleSession(c)
		if s != nil {
			if !s.isReady() {
				closeInvalidSession(ctx, s)
				s = nil

				continue
			}

			return s, nil
		}

		s, err = tryCreateNewSession(ctx, c)
		if s != nil || !isCreateSessionErrorRetriable(err) {
			return s, xerrors.WithStackTrace(err)
		}

		s, err = c.internalPoolWaitFromCh(ctx, o.t)
		if err != nil {
			err = xerrors.WithStackTrace(err)
		}
	}

	return handleNoProgress(s, err, start, c, i)
}

func tryGetIdleSession(c *Client) *session {
	var s *session
	c.mu.WithLock(func() {
		s = c.internalPoolRemoveFirstIdle()
	})

	return s
}

func closeInvalidSession(ctx context.Context, s *session) {
	_ = s.Close(ctx)
}

func tryCreateNewSession(ctx context.Context, c *Client) (*session, error) {
	s, err := c.internalPoolCreateSession(ctx)
	if s == nil && err == nil {
		if err = ctx.Err(); err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		panic("both session and err are nil")
	}

	return s, err
}

func handleNoProgress(s *session, err error, start time.Time, c *Client, attempts int) (*session, error) {
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

		err = xerrors.WithStackTrace(
			fmt.Errorf("failed to get session from pool ("+
				"attempts: %d, latency: %v, pool has %d sessions (%d busy, %d idle, %d create_in_progress): %w",
				attempts, time.Since(start), index, index-idle, idle, createInProgress, err,
			),
		)
	}

	return s, err
}

// Get returns first idle session from the Client and removes it from
// there. If no items stored in Client it creates new one returns it.
func (c *Client) Get(ctx context.Context) (s *session, err error) {
	return c.internalPoolGet(ctx)
}

//nolint:funlen
func (c *Client) internalPoolWaitFromCh(ctx context.Context, t *trace.Table) (s *session, err error) {
	var (
		ch *chan *session
		el *xlist.Element[*chan *session] // Element in the wait queue.
		ok bool
	)

	c.mu.WithLock(func() {
		ch = c.internalPoolGetWaitCh()
		el = c.waitQ.PushBack(ch)
	})

	waitDone := trace.TableOnPoolWait(t, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).internalPoolWaitFromCh"),
	)

	defer func() {
		waitDone(s, err)
	}()

	var createSessionTimeoutCh <-chan time.Time
	if timeout := c.config.CreateSessionTimeout(); timeout > 0 {
		createSessionTimeoutChTimer := c.clock.NewTimer(timeout)
		defer createSessionTimeoutChTimer.Stop()

		createSessionTimeoutCh = createSessionTimeoutChTimer.Chan()
	}

	select {
	case <-c.done:
		c.mu.WithLock(func() {
			c.waitQ.Remove(el)
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

	case <-createSessionTimeoutCh:
		c.mu.WithLock(func() {
			c.waitQ.Remove(el)
		})

		return nil, nil //nolint:nilnil

	case <-ctx.Done():
		c.mu.WithLock(func() {
			c.waitQ.Remove(el)
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
	onDone := trace.TableOnPoolPut(c.config.Trace(), &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).Put"),
		s,
	)
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
			c.internalPoolPushIdle(s, c.clock.Now())
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

			onDone := trace.TableOnClose(c.config.Trace(), &ctx,
				stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).Close"),
			)
			defer func() {
				onDone(err)
			}()

			c.limit = 0

			for el := c.waitQ.Front(); el != nil; el = el.Next() {
				close(*el.Value)
			}

			for e := c.idle.Front(); e != nil; e = e.Next() {
				s := e.Value
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
func (c *Client) Do(ctx context.Context, op table.Operation, opts ...table.Option) (finalErr error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	if c.isClosed() {
		return xerrors.WithStackTrace(errClosedClient)
	}

	config := c.retryOptions(opts...)

	attempts, onDone := 0, trace.TableOnDo(config.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).Do"),
		config.Label, config.Idempotent, xcontext.IsNestedCall(ctx),
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	err := do(ctx, c, c.config, op, func(err error) {
		attempts++
	}, config.RetryOptions...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) DoTx(ctx context.Context, op table.TxOperation, opts ...table.Option) (finalErr error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	if c.isClosed() {
		return xerrors.WithStackTrace(errClosedClient)
	}

	config := c.retryOptions(opts...)

	attempts, onDone := 0, trace.TableOnDoTx(config.Trace, &ctx,
		stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/table.(*Client).DoTx"),
		config.Label, config.Idempotent, xcontext.IsNestedCall(ctx),
	)
	defer func() {
		onDone(attempts, finalErr)
	}()

	return retryBackoff(ctx, c, func(ctx context.Context, s table.Session) (err error) {
		attempts++

		tx, err := s.BeginTransaction(ctx, config.TxSettings)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		defer func() {
			err = handleTransactionError(ctx, tx, err)
		}()

		if err = executeTxOperation(ctx, c, op, tx); err != nil {
			return xerrors.WithStackTrace(err)
		}

		_, err = tx.CommitTx(ctx, config.TxCommitOptions...)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, config.RetryOptions...)
}

func handleTransactionError(ctx context.Context, tx table.Transaction, err error) error {
	if err != nil {
		errRollback := tx.Rollback(ctx)
		if errRollback != nil {
			return xerrors.NewWithIssues("",
				xerrors.WithStackTrace(err),
				xerrors.WithStackTrace(errRollback),
			)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func executeTxOperation(ctx context.Context, c *Client, op table.TxOperation, tx table.Transaction) (err error) {
	if panicCallback := c.config.PanicCallback(); panicCallback != nil {
		defer func() {
			if e := recover(); e != nil {
				panicCallback(e)
			}
		}()
	}

	return op(xcontext.MarkRetryCall(ctx), tx)
}

func (c *Client) internalPoolGCTick(ctx context.Context, idleThreshold time.Duration) {
	c.mu.WithLock(func() {
		if c.isClosed() {
			return
		}
		for e := c.idle.Front(); e != nil; e = e.Next() {
			s := e.Value
			info, has := c.index[s]
			if !has {
				panic("session not found in pool")
			}
			if info.idle == nil {
				panic("inconsistent session info")
			}
			if since := c.clock.Since(info.touched); since > idleThreshold {
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

	timer := c.clock.NewTimer(idleThreshold)
	defer timer.Stop()

	for {
		select {
		case <-c.done:
			return

		case <-ctx.Done():
			return

		case <-timer.Chan():
			c.internalPoolGCTick(ctx, idleThreshold)
			timer.Reset(idleThreshold / 2) //nolint:gomnd
		}
	}
}

// internalPoolGetWaitCh returns pointer to a channel of sessions.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Client.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (c *Client) internalPoolGetWaitCh() *chan *session { //nolint:gocritic
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
func (c *Client) internalPoolPutWaitCh(ch *chan *session) { //nolint:gocritic
	c.waitChPool.Put(ch)
}

// c.mu must be held.
func (c *Client) internalPoolPeekFirstIdle() (s *session, touched time.Time) {
	el := c.idle.Front()
	if el == nil {
		return
	}
	s = el.Value
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
	for el := c.waitQ.Front(); el != nil; el = c.waitQ.Front() {
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
		ch := c.waitQ.Remove(el)
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
	ctx, cancel = xcontext.WithTimeout(ctx, c.config.DeleteTimeout())
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
func (c *Client) internalPoolHandlePushIdle(s *session, now time.Time, el *xlist.Element[*session]) {
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
	idle    *xlist.Element[*session]
	touched time.Time
}
