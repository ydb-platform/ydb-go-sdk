package table

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	errNilClient = xerrors.Wrap(errors.New("table client is not initialized"))

	// errClosedClient returned by a Client instance to indicate
	// that Client is closed early and not able to complete requested operation.
	errClosedClient = xerrors.Wrap(errors.New("table client closed early"))

	// errSessionPoolOverflow returned by a Client instance to indicate
	// that the Client is full and requested operation is not able to complete.
	errSessionPoolOverflow = xerrors.Wrap(errors.New("session pool overflow"))

	// errSessionShutdown returned by a Client instance to indicate that
	// requested session is under shutdown.
	errSessionShutdown = xerrors.Wrap(errors.New("session under shutdown"))

	// errNoProgress returned by a Client instance to indicate that
	// operation could not be completed.
	errNoProgress = xerrors.Wrap(errors.New("no progress"))
)

// SessionBuilder is the interface that holds logic of creating sessions.
type SessionBuilder func(context.Context) (Session, error)

func New(cc grpc.ClientConnInterface, config config.Config) *Client {
	return newClient(cc, func(ctx context.Context) (s Session, err error) {
		return newSession(ctx, cc, config)
	}, config)
}

func newClient(
	cc grpc.ClientConnInterface,
	builder SessionBuilder,
	config config.Config,
) *Client {
	var (
		ctx    = context.Background()
		onDone = trace.TableOnInit(config.Trace(), &ctx)
	)
	c := &Client{
		config: config,
		cc:     cc,
		build:  builder,
		index:  make(map[Session]sessionInfo),
		idle:   list.New(),
		waitq:  list.New(),
		limit:  config.SizeLimit(),
		waitChPool: sync.Pool{
			New: func() interface{} {
				ch := make(chan Session)
				return &ch
			},
		},
	}
	if config.IdleThreshold() > 0 {
		c.keeperStop = make(chan struct{})
		c.keeperDone = make(chan struct{})
		go c.keeper(ctx)
	}
	onDone(c.limit, c.config.KeepAliveMinSize())
	return c
}

// Client is a set of session instances that may be reused.
// A Client is safe for use by multiple goroutines simultaneously.
type Client struct {
	// build holds an object capable for creating sessions.
	// It must not be nil.
	build             SessionBuilder
	cc                grpc.ClientConnInterface
	config            config.Config
	index             map[Session]sessionInfo
	createInProgress  int           // KIKIMR-9163: in-create-process counter
	limit             int           // Upper bound for Client size.
	idle              *list.List    // list<table.session>
	waitq             *list.List    // list<*chan table.session>
	keeperWake        chan struct{} // Set by keeper.
	keeperStop        chan struct{}
	keeperDone        chan struct{}
	touchingDone      chan struct{}
	mu                xsync.Mutex
	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
	spawnedGoroutines background.Worker
	touching          bool
	closed            uint32
}

func (c *Client) CreateSession(ctx context.Context, opts ...table.Option) (_ table.ClosableSession, err error) {
	if c == nil {
		return nil, xerrors.WithStackTrace(errNilClient)
	}
	createSession := func(ctx context.Context) (s Session, err error) {
		type result struct {
			s   Session
			err error
		}

		ch := make(chan result)

		c.spawnedGoroutines.Start("CreateSession", func(ctx context.Context) {
			var (
				s   Session
				err error
			)

			createSessionCtx := xcontext.WithoutDeadline(ctx)

			if timeout := c.config.CreateSessionTimeout(); timeout > 0 {
				var cancel context.CancelFunc
				createSessionCtx, cancel = context.WithTimeout(createSessionCtx, timeout)
				defer cancel()
			}

			s, err = c.build(createSessionCtx)

			select {
			case ch <- result{
				s:   s,
				err: err,
			}: // nop
			case <-ctx.Done():
				if s != nil {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(
						xcontext.WithoutDeadline(ctx),
						c.config.DeleteTimeout(),
					)
					defer cancel()

					_ = s.Close(ctx)
				}
			}
		})

		select {
		case r := <-ch:
			if r.err != nil {
				return nil, xerrors.WithStackTrace(r.err)
			}
			return r.s, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	var s Session
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
			s, err = c.build(ctx)
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
	return atomic.LoadUint32(&c.closed) != 0
}

func isCreateSessionErrorRetriable(err error) bool {
	switch {
	case
		xerrors.Is(err, errSessionPoolOverflow),
		xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED),
		xerrors.IsTransportError(
			err,
			grpcCodes.ResourceExhausted,
			grpcCodes.DeadlineExceeded,
			grpcCodes.Unavailable,
		):
		return true
	default:
		return false
	}
}

type Session interface {
	table.ClosableSession

	Status() string
	OnClose(f func())

	isClosed() bool
	isClosing() bool
}

// p.mu must NOT be held.
func (c *Client) createSession(ctx context.Context) (s Session, err error) {
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

	type result struct {
		s   Session
		err error
	}

	ch := make(chan result)

	c.spawnedGoroutines.Start("createSession", func(ctx context.Context) {
		var (
			s   Session
			err error
		)

		createSessionCtx := xcontext.WithoutDeadline(ctx)

		createSessionCtx = meta.WithAllowFeatures(createSessionCtx,
			meta.HintSessionBalancer,
		)

		if timeout := c.config.CreateSessionTimeout(); timeout > 0 {
			var cancel context.CancelFunc
			createSessionCtx, cancel = context.WithTimeout(createSessionCtx, timeout)
			defer cancel()
		}

		onDone := trace.TableOnPoolSessionNew(c.config.Trace(), &createSessionCtx)

		defer func() {
			onDone(s, err)
		}()

		s, err = c.build(createSessionCtx)
		if s == nil && err == nil {
			panic("ydb: abnormal result of session build")
		}

		if s != nil {
			s.OnClose(func() {
				c.mu.WithLock(func() {
					if c.isClosed() {
						return
					}

					info, has := c.index[s]
					if !has {
						return
					}

					delete(c.index, s)

					trace.TableOnPoolStateChange(c.config.Trace(), len(c.index), "remove")

					c.notify(nil)

					if info.idle != nil {
						panic("session closed while still in idle client")
					}
				})
			})
		}

		c.mu.WithLock(func() {
			c.createInProgress--
			if s != nil {
				if !c.isClosed() {
					c.index[s] = sessionInfo{}
					trace.TableOnPoolStateChange(c.config.Trace(), len(c.index), "append")
				}
			}
		})

		select {
		case ch <- result{
			s:   s,
			err: err,
		}: // nop
		case <-ctx.Done():
			if s != nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(
					xcontext.WithoutDeadline(ctx),
					c.config.DeleteTimeout(),
				)
				defer cancel()

				_ = s.Close(ctx)
			}
		}
	})

	select {
	case r := <-ch:
		if r.err != nil {
			return nil, xerrors.WithStackTrace(r.err)
		}
		return r.s, nil
	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())
	}
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

func (c *Client) get(ctx context.Context, opts ...getOption) (s Session, err error) {
	var (
		i = 0
		o = getOptions{t: c.config.Trace()}
	)
	for _, opt := range opts {
		opt(&o)
	}

	onDone := trace.TableOnPoolGet(o.t, &ctx)
	defer func() {
		onDone(s, i, err)
	}()

	const maxAttempts = 100
	for ; s == nil && err == nil && i < maxAttempts; i++ {
		if c.isClosed() {
			return nil, xerrors.WithStackTrace(errClosedClient)
		}

		// First, we try to get session from idle
		c.mu.WithLock(func() {
			s = c.removeFirstIdle()
		})

		if s != nil {
			return s, nil
		}

		// Second, we try to create new session
		s, err = c.createSession(ctx)
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
		s, err = c.waitFromCh(ctx, o.t)
		if err != nil {
			err = xerrors.WithStackTrace(err)
		}
	}
	if s == nil && err == nil {
		err = xerrors.WithStackTrace(fmt.Errorf("%w: attempts=%d", errNoProgress, i))
	}
	if err != nil {
		return s, xerrors.WithStackTrace(
			fmt.Errorf("%w: attempts=%d", err, i),
		)
	}
	return s, nil
}

// Get returns first idle session from the Client and removes it from
// there. If no items stored in Client it creates new one returns it.
func (c *Client) Get(ctx context.Context) (s Session, err error) {
	return c.get(ctx)
}

func (c *Client) waitFromCh(ctx context.Context, t trace.Table) (s Session, err error) {
	var (
		ch *chan Session
		el *list.Element // Element in the wait queue.
		ok bool
	)

	c.mu.WithLock(func() {
		ch = c.getWaitCh()
		el = c.waitq.PushBack(ch)
	})

	waitDone := trace.TableOnPoolWait(t, &ctx)

	defer func() {
		waitDone(s, err)
	}()

	select {
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
			c.putWaitCh(ch)
		}
		return s, nil

	case <-time.After(c.config.CreateSessionTimeout()):
		c.mu.WithLock(func() {
			c.waitq.Remove(el)
		})
		return s, nil

	case <-ctx.Done():
		c.mu.WithLock(func() {
			c.waitq.Remove(el)
		})
		return nil, ctx.Err()
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
func (c *Client) Put(ctx context.Context, s Session) (err error) {
	onDone := trace.TableOnPoolPut(c.config.Trace(), &ctx, s)
	defer func() {
		onDone(err)
	}()

	c.mu.WithLock(func() {
		switch {
		case c.isClosed():
			err = xerrors.WithStackTrace(errClosedClient)

		case c.idle.Len() >= c.limit:
			err = xerrors.WithStackTrace(errSessionPoolOverflow)

		case s.isClosing():
			err = xerrors.WithStackTrace(errSessionShutdown)

		default:
			if !c.notify(s) {
				c.pushIdle(s, timeutil.Now())
			}
		}
	})

	if err != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(
			xcontext.WithoutDeadline(ctx),
			c.config.DeleteTimeout(),
		)
		defer cancel()

		_ = s.Close(ctx)
	}

	return xerrors.WithStackTrace(err)
}

// Close deletes all stored sessions inside Client.
// It also stops all underlying timers and goroutines.
// It returns first error occurred during stale sessions' deletion.
// Note that even on error it calls Close() on each session.
func (c *Client) Close(ctx context.Context) (err error) {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}

	onDone := trace.TableOnClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	if c.isClosed() {
		return xerrors.WithStackTrace(errClosedClient)
	}

	var issues []error
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		c.mu.WithLock(func() {
			keeperDone := c.keeperDone
			if ch := c.keeperStop; ch != nil {
				close(ch)
			}

			if keeperDone != nil {
				<-keeperDone
			}

			for el := c.waitq.Front(); el != nil; el = el.Next() {
				ch := el.Value.(*chan Session)
				close(*ch)
			}

			issues = make([]error, 0, len(c.index))
			for e := c.idle.Front(); e != nil; e = e.Next() {
				if err = c.closeSession(
					ctx,
					e.Value.(Session),
					withCloseSessionAsync(),
				); err != nil {
					issues = append(issues, err)
				}
			}

			c.limit = 0
			c.idle = list.New()
			c.waitq = list.New()
			c.index = make(map[Session]sessionInfo)
		})
	}

	_ = c.spawnedGoroutines.Close(ctx, errClosedClient)

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("table client closed with issues", issues...))
	}

	return nil
}

func retryOptions(trace trace.Table, opts ...table.Option) table.Options {
	options := table.Options{
		Trace:       trace,
		FastBackoff: backoff.Fast,
		SlowBackoff: backoff.Slow,
		TxSettings: table.TxSettings(
			table.WithSerializableReadWrite(),
		),
	}
	for _, o := range opts {
		o(&options)
	}
	return options
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
	return doTx(
		ctx,
		c,
		c.config,
		op,
		retryOptions(c.config.Trace(), opts...),
	)
}

func (c *Client) keeper(ctx context.Context) {
	defer close(c.keeperDone)
	var (
		toTouch    []Session // Cached for reuse.
		toDelete   []Session // Cached for reuse.
		toTryAgain []Session // Cached for reuse.

		wake  = make(chan struct{})
		timer = timeutil.NewTimer(c.config.IdleThreshold())
	)

	for {
		var now time.Time
		select {
		case <-wake:
			wake = make(chan struct{})
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			timer.Reset(c.config.IdleThreshold())
			continue

		case <-c.keeperStop:
			return

		case now = <-timer.C():
			toTouch = toTouch[:0]
			toTryAgain = toTryAgain[:0]
			toDelete = toDelete[:0]

			c.mu.WithLock(func() {
				c.touching = true
				for c.idle.Len() > 0 {
					s, touched := c.peekFirstIdle()
					if s == nil || now.Sub(touched) < c.config.IdleThreshold() {
						break
					}
					_ = c.removeIdle(s)
					toTouch = append(toTouch, s)
				}
			})

			var mark *list.Element // Element in the list to insert touched sessions after.
			for i, s := range toTouch {
				toTouch[i] = nil

				var keepAliveCount, lenIndex int
				c.mu.WithLock(func() {
					keepAliveCount = c.incrementKeepAlive(s)
					lenIndex = len(c.index)
				})

				// if keepAlive was called more than the corresponding limit for the
				// session to be alive and more sessions are open than the lower limit
				// of continuously kept sessions
				if c.config.IdleKeepAliveThreshold() > 0 {
					if keepAliveCount >= c.config.IdleKeepAliveThreshold() {
						if c.config.KeepAliveMinSize() < lenIndex-len(toDelete) {
							toDelete = append(toDelete, s)
							continue
						}
					}
				}

				err := c.keepAliveSession(context.Background(), s)
				if err != nil {
					switch {
					case
						xerrors.Is(
							err,
							balancer.ErrNoEndpoints,
						),
						xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION),
						xerrors.IsTransportError(
							err,
							grpcCodes.DeadlineExceeded,
							grpcCodes.Unavailable,
						):
						toDelete = append(toDelete, s)
					default:
						toTryAgain = append(toTryAgain, s)
					}
					continue
				}

				c.mu.WithLock(func() {
					if !c.notify(s) {
						// Need to push back session into list in order, to prevent
						// shuffling of sessions order.
						//
						// That is, there may be a race condition, when some session S1
						// pushed back in the list before we took the mutex. Suppose S1
						// touched time is greater than ours `now` for S0. If so, it
						// then may interrupt next keep alive iteration earlier and
						// prevent our session S0 being touched:
						// time.Since(S1) < threshold but time.Since(S0) > threshold.
						mark = c.pushIdleInOrderAfter(s, now, mark)
					}
				})
			}

			{ // push all the soft failed sessions to retry on the next tick
				pushBackTime := now.Add(-c.config.IdleThreshold())

				c.mu.WithLock(func() {
					for _, el := range toTryAgain {
						_ = c.pushIdleInOrder(el, pushBackTime)
					}
				})
			}

			var (
				sleep bool
				delay time.Duration
			)

			var touchingDone chan struct{}
			c.mu.WithLock(func() {
				if s, touched := c.peekFirstIdle(); s == nil {
					// No sessions to check. Let the Put() caller to wake up
					// keeper when session arrive.
					sleep = true
					c.keeperWake = wake
				} else {
					// NOTE: negative delay is also fine.
					delay = c.config.IdleThreshold() - now.Sub(touched)
				}

				// Takers notification broadcast channel.
				touchingDone = c.touchingDone
				c.touchingDone = nil
				c.touching = false
			})

			if !sleep {
				timer.Reset(delay)
			}
			for _, s := range toDelete {
				_ = c.closeSession(ctx, s)
			}
			if touchingDone != nil {
				close(touchingDone)
			}
		}
	}
}

// getWaitCh returns pointer to a channel of sessions.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Client.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (c *Client) getWaitCh() *chan Session {
	if c.testHookGetWaitCh != nil {
		c.testHookGetWaitCh()
	}
	ch := c.waitChPool.Get()
	s, ok := ch.(*chan Session)
	if !ok {
		panic(fmt.Sprintf("%T is not a chan of sessions", ch))
	}
	return s
}

// putWaitCh receives pointer to a channel and makes it available for further
// use.
// Note that ch MUST NOT be owned by any goroutine at the call moment and ch
// MUST NOT contain any value.
func (c *Client) putWaitCh(ch *chan Session) {
	c.waitChPool.Put(ch)
}

// p.mu must be held.
func (c *Client) peekFirstIdle() (s Session, touched time.Time) {
	el := c.idle.Front()
	if el == nil {
		return
	}
	s = el.Value.(Session)
	info, has := c.index[s]
	if !has || el != info.idle {
		panic("inconsistent session client index")
	}
	return s, info.touched
}

// removes first session from idle and resets the keepAliveCount
// to prevent session from dying in the keeper after it was returned
// to be used only in outgoing functions that make session busy.
// p.mu must be held.
func (c *Client) removeFirstIdle() Session {
	s, _ := c.peekFirstIdle()
	if s != nil {
		info := c.removeIdle(s)
		info.keepAliveCount = 0
		c.index[s] = info
	}
	return s
}

// Increments the Keep Alive Counter and returns the previous number.
// Unlike other info modifiers, this one doesn't care if it didn't find the session, it skips
// the action. You can still check it later if needed, if the return code is -1
// p.mu must be held.
func (c *Client) incrementKeepAlive(s Session) int {
	info, has := c.index[s]
	if !has {
		return -1
	}
	ret := info.keepAliveCount
	info.keepAliveCount++
	c.index[s] = info
	return ret
}

// p.mu must be held.
func (c *Client) touchCond() <-chan struct{} {
	if c.touchingDone == nil {
		c.touchingDone = make(chan struct{})
	}
	return c.touchingDone
}

// p.mu must be held.
func (c *Client) notify(s Session) (notified bool) {
	for el := c.waitq.Front(); el != nil; el = c.waitq.Front() {
		// Some goroutine is waiting for a session.
		//
		// It could be in this states:
		//   1) Reached the select code and awaiting for a value in channel.
		//   2) Reached the select code but already in branch of deadline
		//   cancellation. In this case it is locked on p.mu.Lock().
		//   3) Not reached the select code and thus not reading yet from the
		//   channel.
		//
		// For cases (2) and (3) we close the channel to signal that goroutine
		// missed something and may want to retry (especially for case (3)).
		//
		// After that we taking a next waiter and repeat the same.
		ch := c.waitq.Remove(el).(*chan Session)
		select {
		case *ch <- s:
			// Case (1).
			return true
		default:
			// Case (2) or (3).
			close(*ch)
		}
	}
	return false
}

// CloseSession provides the most effective way of session closing
// instead of plain session.Close().
// CloseSession must be fast. If necessary, can be async.
func (c *Client) CloseSession(ctx context.Context, s Session) error {
	return c.closeSession(
		ctx,
		s,
		withCloseSessionAsync(),
		withCloseSessionTrace(),
	)
}

type closeSessionOptionsHolder struct {
	withTrace bool
	withAsync bool
}

type closeSessionOption func(h *closeSessionOptionsHolder)

func withCloseSessionAsync() closeSessionOption {
	return func(h *closeSessionOptionsHolder) {
		h.withAsync = true
	}
}

func withCloseSessionTrace() closeSessionOption {
	return func(h *closeSessionOptionsHolder) {
		h.withTrace = true
	}
}

// closeSession is an async func which close session, but without `trace.OnPoolSessionClose` tracing
func (c *Client) closeSession(ctx context.Context, s Session, opts ...closeSessionOption) error {
	h := closeSessionOptionsHolder{}

	for _, o := range opts {
		o(&h)
	}

	if h.withTrace {
		onDone := trace.TableOnPoolSessionClose(c.config.Trace(), &ctx, s)
		defer onDone()
	}

	f := func(s Session) {
		closeCtx, cancel := context.WithTimeout(
			xcontext.WithoutDeadline(ctx),
			c.config.DeleteTimeout(),
		)
		defer cancel()

		_ = s.Close(closeCtx)
	}

	if h.withAsync {
		c.spawnedGoroutines.Start("closeSession", func(ctx context.Context) {
			f(s)
		})
	} else {
		f(s)
	}

	return nil
}

func (c *Client) keepAliveSession(ctx context.Context, s Session) (err error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.KeepAliveTimeout())
	defer cancel()
	err = s.KeepAlive(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}
	return nil
}

// p.mu must be held.
func (c *Client) removeIdle(s Session) sessionInfo {
	info, has := c.index[s]
	if !has || info.idle == nil {
		panic("inconsistent session client index")
	}

	c.idle.Remove(info.idle)
	info.idle = nil
	c.index[s] = info
	return info
}

// p.mu must be held.
func (c *Client) pushIdle(s Session, now time.Time) {
	c.handlePushIdle(s, now, c.idle.PushBack(s))
}

// p.mu must be held.
func (c *Client) pushIdleInOrder(s Session, now time.Time) (el *list.Element) {
	var prev *list.Element
	for prev = c.idle.Back(); prev != nil; prev = prev.Prev() {
		s := prev.Value.(Session)
		t := c.index[s].touched
		if !now.Before(t) { // now >= t
			break
		}
	}
	if prev != nil {
		el = c.idle.InsertAfter(s, prev)
	} else {
		el = c.idle.PushFront(s)
	}
	c.handlePushIdle(s, now, el)
	return el
}

// p.mu must be held.
func (c *Client) pushIdleInOrderAfter(s Session, now time.Time, mark *list.Element) *list.Element {
	if mark != nil {
		n := c.idle.Len()
		el := c.idle.InsertAfter(s, mark)
		if n < c.idle.Len() {
			// List changed, thus mark belongs to list.
			c.handlePushIdle(s, now, el)
			return el
		}
	}
	return c.pushIdleInOrder(s, now)
}

// p.mu must be held.
func (c *Client) handlePushIdle(s Session, now time.Time, el *list.Element) {
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

	c.wakeUpKeeper()
}

// p.mu must be held.
func (c *Client) wakeUpKeeper() {
	if wake := c.keeperWake; wake != nil {
		c.keeperWake = nil
		close(wake)
	}
}

type sessionInfo struct {
	idle           *list.Element
	touched        time.Time
	keepAliveCount int
}
