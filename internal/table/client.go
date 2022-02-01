package table

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/deadline"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	// ErrSessionPoolClosed is returned by a client instance to indicate
	// that client is closed and not able to complete requested operation.
	ErrSessionPoolClosed = errors.New("ydb: table: session pool is closed")

	// ErrSessionPoolOverflow is returned by a client instance to indicate
	// that the client is full and requested operation is not able to complete.
	ErrSessionPoolOverflow = errors.New("ydb: table: session pool overflow")

	// ErrSessionUnknown is returned by a client instance to indicate that
	// requested session does not exist within the client.
	ErrSessionUnknown = errors.New("ydb: table: unknown session")
)

// ErrNoProgress is returned by a client instance to indicate that
// operation could not be completed.
func ErrNoProgress(attempts int) error {
	return fmt.Errorf("ydb: table: no progress (%d attempts)", attempts)
}

// SessionBuilder is the interface that holds logic of creating sessions.
type SessionBuilder func(context.Context) (Session, error)

type Client interface {
	table.Client

	Get(ctx context.Context) (s Session, err error)
	Put(ctx context.Context, s Session) (err error)
	CloseSession(ctx context.Context, s Session) (err error)
}

func New(ctx context.Context, cc grpc.ClientConnInterface, opts ...config.Option) Client {
	config := config.New(opts...)
	return newClient(ctx, cc, nil, config)
}

func newClient(
	ctx context.Context,
	cc grpc.ClientConnInterface,
	builder SessionBuilder,
	config config.Config,
) *client {
	onDone := trace.TableOnPoolInit(config.Trace().Compose(trace.ContextTable(ctx)), &ctx)
	if builder == nil {
		builder = func(ctx context.Context) (s Session, err error) {
			return newSession(ctx, cc, config.Trace().Compose(trace.ContextTable(ctx)))
		}
	}
	c := &client{
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
		go c.keeper()
	}
	onDone(c.limit, c.config.KeepAliveMinSize())
	return c
}

// client is a set of session instances that may be reused.
// A client is safe for use by multiple goroutines simultaneously.
type client struct {
	// build holds an object capable for creating sessions.
	// It must not be nil.
	build             SessionBuilder
	cc                grpc.ClientConnInterface
	config            config.Config
	index             map[Session]sessionInfo
	createInProgress  int           // KIKIMR-9163: in-create-process counter
	limit             int           // Upper bound for client size.
	idle              *list.List    // list<table.session>
	waitq             *list.List    // list<*chan table.session>
	keeperWake        chan struct{} // Set by keeper.
	keeperStop        chan struct{}
	keeperDone        chan struct{}
	touchingDone      chan struct{}
	mu                sync.Mutex
	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
	wgClosed          sync.WaitGroup
	touching          bool
	closed            bool
}

func (c *client) CreateSession(ctx context.Context) (s table.ClosableSession, err error) {
	return c.build(ctx)
}

func (c *client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func isCreateSessionErrorRetriable(err error) bool {
	switch {
	case
		errors.Is(err, errors.ErrNilConnection),
		errors.Is(err, ErrSessionPoolOverflow),
		errors.IsOpError(err, errors.StatusOverloaded),
		errors.IsTransportError(err, errors.TransportErrorResourceExhausted),
		errors.IsTransportError(err, errors.TransportErrorDeadlineExceeded),
		errors.IsTransportError(err, errors.TransportErrorUnavailable):
		return true
	default:
		return false
	}
}

type Session interface {
	table.ClosableSession

	IsClosed() bool
	Status() string
	OnClose(f func(ctx context.Context))
}

type createSessionResult struct {
	s   Session
	err error
}

// p.mu must NOT be held.
func (c *client) createSession(ctx context.Context) (s Session, err error) {
	// pre-check the client size
	c.mu.Lock()
	enoughSpace := c.createInProgress+len(c.index) < c.limit
	if enoughSpace {
		c.createInProgress++
	}
	c.mu.Unlock()

	if !enoughSpace {
		return nil, ErrSessionPoolOverflow
	}

	resCh := make(chan createSessionResult, 1) // for non-block write

	go func() {
		var (
			r createSessionResult
			t = c.config.Trace().Compose(trace.ContextTable(ctx))
		)

		onDone := trace.TableOnPoolSessionNew(t, &ctx)
		defer func() {
			onDone(r.s, r.err)
		}()

		createSessionCtx, cancel := context.WithTimeout(
			deadline.ContextWithoutDeadline(ctx),
			c.config.CreateSessionTimeout(),
		)

		defer func() {
			cancel()
			close(resCh)
		}()

		r.s, r.err = c.build(createSessionCtx)
		// if session not nil - error must be nil and vice versa
		if r.s == nil && r.err == nil {
			panic("ydb: abnormal result of session build")
		}

		if r.err != nil {
			c.mu.Lock()
			c.createInProgress--
			c.mu.Unlock()
			resCh <- r
			return
		}

		r.s.OnClose(func(ctx context.Context) {
			c.mu.Lock()
			defer c.mu.Unlock()
			info, has := c.index[r.s]
			if !has {
				return
			}

			delete(c.index, r.s)
			c.notify(nil)

			if info.idle != nil {
				panic("ydb: table: session closed while still in idle client")
			}
		})

		// Slot for session already reserved early
		c.mu.Lock()
		c.index[r.s] = sessionInfo{}
		c.createInProgress--
		c.mu.Unlock()

		resCh <- r
	}()

	select {
	case r := <-resCh:
		if r.s == nil && r.err == nil {
			panic("ydb: abnormal result of client.createSession()")
		}
		return r.s, r.err
	case <-ctx.Done():
		// read result from resCh for prevention of forgetting session
		go func() {
			if r, ok := <-resCh; ok && r.s != nil {
				_ = r.s.Close(ctx)
			}
		}()
		return nil, ctx.Err()
	}
}

// Get returns first idle session from the client and removes it from
// there. If no items stored in client it creates new one returns it.
func (c *client) Get(ctx context.Context) (s Session, err error) {
	var (
		i = 0
		t = c.config.Trace().Compose(trace.ContextTable(ctx))
	)

	onDone := trace.TableOnPoolGet(t, &ctx)
	defer func() {
		onDone(s, i, err)
	}()

	const maxAttempts = 100
	for ; s == nil && err == nil && i < maxAttempts; i++ {
		if c.isClosed() {
			return nil, ErrSessionPoolClosed
		}

		// First, we try to get session from idle
		c.mu.Lock()
		s = c.removeFirstIdle()
		c.mu.Unlock()

		if s != nil {
			return s, nil
		}

		// Second, we try to create new session
		s, err = c.createSession(ctx)
		if s == nil && err == nil {
			panic("both of session and err are nil")
		}
		// got session or err is not recoverable
		if s != nil || !isCreateSessionErrorRetriable(err) {
			return s, err
		}

		// Third, we try to wait for a touched session - client is full.
		//
		// This should be done only if number of currently waiting goroutines
		// are less than maximum amount of touched session. That is, we want to
		// be fair here and not to lock more goroutines than we could ship
		// session to.
		s, err = c.waitFromCh(ctx, t)
	}
	if s == nil && err == nil {
		err = ErrNoProgress(i)
	}
	return s, err
}

func (c *client) waitFromCh(ctx context.Context, t trace.Table) (s Session, err error) {
	var (
		ch *chan Session
		el *list.Element // Element in the wait queue.
		ok bool
	)

	c.mu.Lock()
	ch = c.getWaitCh()
	el = c.waitq.PushBack(ch)
	c.mu.Unlock()

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
			// Put only filled and not closed channel back to the client.
			// That is, we need to avoid races on filling reused channel
			// for the next waiter – session could be lost for a long time.
			c.putWaitCh(ch)
		}
		return s, nil

	case <-time.After(c.config.CreateSessionTimeout()):
		c.mu.Lock()
		c.waitq.Remove(el)
		c.mu.Unlock()
		return s, nil

	case <-ctx.Done():
		c.mu.Lock()
		c.waitq.Remove(el)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Put returns session to the client for further reuse.
// If client is already closed Put() calls s.Close(ctx) and returns
// ErrSessionPoolClosed.
// If client is overflow calls s.Close(ctx) and returns
// ErrSessionPoolOverflow.
//
// Note that Put() must be called only once after being created or received by
// Get() or Take() calls. In other way it will produce unexpected behavior or
// panic.
func (c *client) Put(ctx context.Context, s Session) (err error) {
	onDone := trace.TableOnPoolPut(c.config.Trace().Compose(trace.ContextTable(ctx)), &ctx, s)
	defer func() {
		onDone(err)
	}()

	c.mu.Lock()
	switch {
	case c.closed:
		err = ErrSessionPoolClosed

	case c.idle.Len() >= c.limit:
		err = ErrSessionPoolOverflow

	default:
		if !c.notify(s) {
			c.pushIdle(s, timeutil.Now())
		}
	}
	c.mu.Unlock()

	if err != nil {
		_ = c.CloseSession(ctx, s)
	}

	return
}

// Take removes session from the client and ensures that s will not be returned
// by other Take() or Get() calls.
//
// The intended way of Take() use is to create session by calling Create() and
// Put() it later to prepare KeepAlive tracking when session is idle. When
// session becomes active, one should call Take() to stop KeepAlive tracking
// (simultaneous use of session is prohibited).
//
// After session returned to the client by calling PutBusy() it can not be taken
// by Take() any more. That is, semantically PutBusy() is the same as session's
// Close().
//
// It is assumed that Take() callers never call Get() method.
func (c *client) Take(ctx context.Context, s Session) (took bool, err error) {
	onWait := trace.TableOnPoolTake(c.config.Trace().Compose(trace.ContextTable(ctx)), &ctx, s)
	var onDone func(took bool, _ error)
	defer func() {
		if onDone == nil {
			onDone = onWait()
		}
		onDone(took, err)
	}()

	if c.isClosed() {
		return false, ErrSessionPoolClosed
	}

	var has bool
	c.mu.Lock()
	for has, took = c.takeIdle(s); has && !took && c.touching; has, took = c.takeIdle(s) {
		cond := c.touchCond()
		c.mu.Unlock()
		onDone = onWait()

		// Keepalive processing takes place right now.
		// Try to await touched session before creation of new one.
		select {
		case <-cond:

		case <-ctx.Done():
			return false, ctx.Err()
		}

		c.mu.Lock()
	}
	c.mu.Unlock()

	if !has {
		err = ErrSessionUnknown
	}

	return took, err
}

// Create creates new session and returns it.
// The intended way of Create() usage relates to Take() method.
func (c *client) Create(ctx context.Context) (s Session, err error) {
	const maxAttempts = 10
	i := 0
	for ; i < maxAttempts; i++ {
		c.mu.Lock()
		// NOTE: here is a race condition with keeper() running.
		// session could be deleted by some reason after we released the mutex.
		// We are not dealing with this because even if session is not deleted
		// by keeper() it could be staled on the server and the same user
		// experience will appear.
		s, _ = c.peekFirstIdle()
		c.mu.Unlock()

		if s == nil {
			return c.createSession(ctx)
		}

		took, e := c.Take(ctx, s)
		if e == nil && !took || errors.Is(e, ErrSessionUnknown) {
			// session was marked for deletion or deleted by keeper() - race happen - retry
			s = nil
			continue
		}
		if e != nil {
			return nil, e
		}

		return s, nil
	}

	return nil, ErrNoProgress(i)
}

// Close deletes all stored sessions inside client.
// It also stops all underlying timers and goroutines.
// It returns first error occurred during stale sessions' deletion.
// Note that even on error it calls Close() on each session.
func (c *client) Close(ctx context.Context) (err error) {
	onDone := trace.TableOnPoolClose(c.config.Trace().Compose(trace.ContextTable(ctx)), &ctx)
	defer func() {
		onDone(err)
	}()

	if c.isClosed() {
		return
	}

	c.mu.Lock()
	c.closed = true
	keeperDone := c.keeperDone
	if ch := c.keeperStop; ch != nil {
		close(ch)
	}
	c.mu.Unlock()

	if keeperDone != nil {
		<-keeperDone
	}

	c.mu.Lock()
	idle := c.idle
	waitq := c.waitq
	c.limit = 0
	c.idle = list.New()
	c.waitq = list.New()
	c.index = make(map[Session]sessionInfo)
	c.mu.Unlock()

	for el := waitq.Front(); el != nil; el = el.Next() {
		ch := el.Value.(*chan Session)
		close(*ch)
	}
	for e := idle.Front(); e != nil; e = e.Next() {
		s := e.Value.(Session)
		func() {
			closeCtx, cancel := context.WithTimeout(deadline.ContextWithoutDeadline(ctx), c.config.DeleteTimeout())
			_ = s.Close(closeCtx)
			cancel()
		}()
	}
	c.wgClosed.Wait()

	return nil
}

// Do provide the best effort for execute operation
// Do implements internal busy loop until one of the following conditions is met:
// - deadline was canceled or deadlined
// - retry operation returned nil as error
// Warning: if deadline without deadline or cancellation func Retry will be worked infinite
func (c *client) Do(ctx context.Context, op table.Operation, opts ...table.Option) (err error) {
	if c.isClosed() {
		return ErrSessionPoolClosed
	}
	return do(
		ctx,
		c,
		op,
		withOptions(opts...),
		withTrace(c.config.Trace()),
	)
}

func (c *client) DoTx(ctx context.Context, op table.TxOperation, opts ...table.Option) (err error) {
	if c.isClosed() {
		return ErrSessionPoolClosed
	}
	if c.isClosed() {
		return ErrSessionPoolClosed
	}
	return doTx(
		ctx,
		c,
		op,
		withOptions(opts...),
		withTrace(c.config.Trace()),
	)
}

func (c *client) Stats() poolStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	idleCount, waitQCount, indexCount := 0, 0, 0
	if c.idle != nil {
		idleCount = c.idle.Len()
	}
	if c.waitq != nil {
		waitQCount = c.waitq.Len()
	}
	if c.index != nil {
		indexCount = len(c.index)
	}
	return poolStats{
		Idle:             idleCount,
		Index:            indexCount,
		WaitQ:            waitQCount,
		CreateInProgress: c.createInProgress,
		MinSize:          c.config.KeepAliveMinSize(),
		MaxSize:          c.limit,
	}
}

func (c *client) keeper() {
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

			c.mu.Lock()
			{
				c.touching = true
				for c.idle.Len() > 0 {
					s, touched := c.peekFirstIdle()
					if s == nil || now.Sub(touched) < c.config.IdleThreshold() {
						break
					}
					_ = c.removeIdle(s)
					toTouch = append(toTouch, s)
				}
			}
			c.mu.Unlock()

			var mark *list.Element // Element in the list to insert touched sessions after.
			for i, s := range toTouch {
				toTouch[i] = nil

				c.mu.Lock()
				keepAliveCount := c.incrementKeepAlive(s)
				lenIndex := len(c.index)
				c.mu.Unlock()
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
						errors.IsOpError(err, errors.StatusBadSession),
						errors.IsTransportError(err, errors.TransportErrorDeadlineExceeded):
						toDelete = append(toDelete, s)
					default:
						toTryAgain = append(toTryAgain, s)
					}
					continue
				}

				c.mu.Lock()
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
				c.mu.Unlock()
			}

			{ // push all the soft failed sessions to retry on the next tick
				pushBackTime := now.Add(-c.config.IdleThreshold())

				c.mu.Lock()
				for _, el := range toTryAgain {
					_ = c.pushIdleInOrder(el, pushBackTime)
				}
				c.mu.Unlock()
			}

			var (
				sleep bool
				delay time.Duration
			)
			c.mu.Lock()

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
			touchingDone := c.touchingDone
			c.touchingDone = nil
			c.touching = false

			c.mu.Unlock()

			if !sleep {
				timer.Reset(delay)
			}
			for i, s := range toDelete {
				toDelete[i] = nil
				ctx, cancel := context.WithTimeout(
					context.Background(),
					c.config.DeleteTimeout(),
				)
				_ = s.Close(ctx)
				cancel()
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
func (c *client) getWaitCh() *chan Session {
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
func (c *client) putWaitCh(ch *chan Session) {
	c.waitChPool.Put(ch)
}

// p.mu must be held.
func (c *client) peekFirstIdle() (s Session, touched time.Time) {
	el := c.idle.Front()
	if el == nil {
		return
	}
	s = el.Value.(Session)
	info, has := c.index[s]
	if !has || el != info.idle {
		panicLocked(&c.mu, "ydb: table: inconsistent session client index")
	}
	return s, info.touched
}

// removes first session from idle and resets the keepAliveCount
// to prevent session from dying in the keeper after it was returned
// to be used only in outgoing functions that make session busy.
// p.mu must be held.
func (c *client) removeFirstIdle() Session {
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
func (c *client) incrementKeepAlive(s Session) int {
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
func (c *client) touchCond() <-chan struct{} {
	if c.touchingDone == nil {
		c.touchingDone = make(chan struct{})
	}
	return c.touchingDone
}

// p.mu must be held.
func (c *client) notify(s Session) (notified bool) {
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
func (c *client) CloseSession(ctx context.Context, s Session) error {
	c.wgClosed.Add(1)
	onDone := trace.TableOnPoolSessionClose(c.config.Trace().Compose(trace.ContextTable(ctx)), &ctx, s)
	defer onDone()
	fn := func() {
		defer c.wgClosed.Done()
		closeCtx, cancel := context.WithTimeout(
			deadline.ContextWithoutDeadline(ctx),
			c.config.DeleteTimeout(),
		)
		defer cancel()
		_ = s.Close(closeCtx)
	}
	if c.isClosed() {
		fn()
	} else {
		go fn()
	}
	return nil
}

func (c *client) keepAliveSession(ctx context.Context, s Session) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.KeepAliveTimeout())
	defer cancel()
	return s.KeepAlive(ctx)
}

// p.mu must be held.
func (c *client) removeIdle(s Session) sessionInfo {
	info, has := c.index[s]
	if !has || info.idle == nil {
		panicLocked(&c.mu, "ydb: table: inconsistent session client index")
	}

	c.idle.Remove(info.idle)
	info.idle = nil
	c.index[s] = info
	return info
}

// Removes session from idle client and resets keepAliveCount for it not
// to die in keeper when it will be returned
// to be used only in outgoing functions that make session busy.
// p.mu must be held.
func (c *client) takeIdle(s Session) (has, took bool) {
	var info sessionInfo
	info, has = c.index[s]
	if !has {
		// Could not be strict here and panic – session may become deleted by
		// keeper().
		return
	}
	if info.idle == nil {
		// session s is not idle.
		return
	}
	took = true
	info = c.removeIdle(s)
	info.keepAliveCount = 0
	c.index[s] = info
	return
}

// p.mu must be held.
func (c *client) pushIdle(s Session, now time.Time) {
	c.handlePushIdle(s, now, c.idle.PushBack(s))
}

// p.mu must be held.
func (c *client) pushIdleInOrder(s Session, now time.Time) (el *list.Element) {
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
func (c *client) pushIdleInOrderAfter(s Session, now time.Time, mark *list.Element) *list.Element {
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
func (c *client) handlePushIdle(s Session, now time.Time, el *list.Element) {
	info, has := c.index[s]
	if !has {
		panicLocked(&c.mu, "ydb: table: trying to store session created outside of the client")
	}
	if info.idle != nil {
		panicLocked(&c.mu, "ydb: table: inconsistent session client index")
	}

	info.touched = now
	info.idle = el
	c.index[s] = info

	c.wakeUpKeeper()
}

// p.mu must be held.
func (c *client) wakeUpKeeper() {
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

func panicLocked(mu sync.Locker, message string) {
	mu.Unlock()
	panic(message)
}

type poolStats struct {
	Idle             int
	Index            int
	WaitQ            int
	MinSize          int
	MaxSize          int
	CreateInProgress int
}
