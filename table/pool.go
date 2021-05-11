package table

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

var (
	DefaultSessionPoolKeepAliveTimeout     = 500 * time.Millisecond
	DefaultSessionPoolDeleteTimeout        = 500 * time.Millisecond
	DefaultSessionPoolCreateSessionTimeout = 5 * time.Second
	DefaultSessionPoolIdleThreshold        = 5 * time.Minute
	DefaultSessionPoolBusyCheckInterval    = 1 * time.Second
	DefaultSessionPoolSizeLimit            = 50
	DefaultKeepAliveMinSize                = 10
	DefaultIdleKeepAliveThreshold          = 2
)

var (
	// ErrSessionPoolClosed is returned by a SessionPool instance to indicate
	// that pool is closed and not able to complete requested operation.
	ErrSessionPoolClosed = errors.New("ydb: table: session pool is closed")

	// ErrSessionPoolOverflow is returned by a SessionPool instance to indicate
	// that the pool is full and requested operation is not able to complete.
	ErrSessionPoolOverflow = errors.New("ydb: table: session pool overflow")

	// ErrSessionUnknown is returned by a SessionPool instance to indicate that
	// requested session does not exist within the pool.
	ErrSessionUnknown = errors.New("ydb: table: unknown session")

	// ErrNoProgress is returned by a SessionPool instance to indicate that
	// operation could not be completed.
	ErrNoProgress = errors.New("ydb: table: no progress")
)

// SessionBuilder is the interface that holds logic of creating or deleting
// sessions.
type SessionBuilder interface {
	CreateSession(context.Context) (*Session, error)
}

// SessionPool is a set of Session instances that may be reused.
// A SessionPool is safe for use by multiple goroutines simultaneously.
type SessionPool struct {
	// Trace is an optional session lifetime tracing options.
	Trace SessionPoolTrace

	// Builder holds an object capable for creating and deleting sessions.
	// It must not be nil.
	Builder SessionBuilder

	// SizeLimit is an upper bound of pooled sessions.
	// If SizeLimit is less than or equal to zero then the
	// DefaultSessionPoolSizeLimit variable is used as a limit.
	SizeLimit int

	// KeepAliveMinSize is a lower bound for sessions in the pool. If there are more sessions open, then
	// the excess idle ones will be closed and removed after IdleKeepAliveThreshold is reached for each of them.
	// If KeepAliveMinSize is less than zero, then no sessions will be preserved
	// If KeepAliveMinSize is zero, the DefaultKeepAliveMinSize is used
	KeepAliveMinSize int

	// IdleKeepAliveThreshold is a number of keepAlive messages to call before the
	// Session is removed if it is an excess session (see KeepAliveMinSize)
	// This means session lifetime = IdleThreshold * IdleKeepAliveThreshold
	// If IdleKeepAliveThreshold is less than zero then it will be treated as infinite and no sessions will
	// be removed ever.
	// If IdleKeepAliveThreshold is equal to zero, it will be set to DefaultIdleKeepAliveThreshold
	IdleKeepAliveThreshold int

	// IdleLimit is an upper bound of pooled sessions without any activity
	// within.
	// IdleLimit int

	// IdleThreshold is a maximum duration between any activity within session.
	// If this threshold reached, KeepAlive() method will be called on idle
	// session.
	//
	// If IdleThreshold is less than zero then there is no idle limit.
	// If IdleThreshold is zero, then the DefaultSessionPoolIdleThreshold value
	// is used.
	IdleThreshold time.Duration

	// BusyCheckInterval is an interval between busy sessions status checks.
	// If BusyCheckInterval is less than zero then there busy checking is
	// disabled.
	// If BusyCheckInterval is equal to zero, then the
	// DefaultSessionPoolBusyCheckInterval value is used.
	BusyCheckInterval time.Duration

	// KeepAliveBatchSize is a maximum number sessions taken from the pool to
	// prepare KeepAlive() call on them in background.
	// If KeepAliveBatchSize is less than or equal to zero, then there is no
	// batch limit.
	KeepAliveBatchSize int

	// KeepAliveTimeout limits maximum time spent on KeepAlive request for
	// KeepAliveBatchSize number of sessions.
	// If KeepAliveTimeout is less than or equal to zero then the
	// DefaultSessionPoolKeepAliveTimeout is used.
	KeepAliveTimeout time.Duration

	// CreateSessionTimeout limits maximum time spent on Create session request
	// If CreateSessionTimeout is less than or equal to zero then the
	// DefaultSessionPoolCreateSessionTimeout is used.
	CreateSessionTimeout time.Duration

	// DeleteTimeout limits maximum time spent on Delete request for
	// KeepAliveBatchSize number of sessions.
	// If DeleteTimeout is less than or equal to zero then the
	// DefaultSessionPoolDeleteTimeout is used.
	DeleteTimeout time.Duration

	mu               sync.Mutex
	initOnce         sync.Once
	index            map[*Session]sessionInfo
	createInProgress int        // KIKIMR-9163: in-create-process counter
	limit            int        // Upper bound for pool size.
	idle             *list.List // list<*Session>
	ready            *list.List // list<*Session>
	waitq            *list.List // list<*chan *Session>

	keeperWake chan struct{} // Set by keeper.
	keeperStop chan struct{}
	keeperDone chan struct{}

	touching     bool
	touchingDone chan struct{}

	busyCheck       chan *Session
	busyCheckerStop chan struct{}
	busyCheckerDone chan struct{}

	closed bool

	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
}

func (p *SessionPool) init() {
	p.initOnce.Do(func() {
		p.index = make(map[*Session]sessionInfo)

		p.idle = list.New()
		p.ready = list.New()
		p.waitq = list.New()
		p.limit = p.SizeLimit
		if p.limit <= 0 {
			p.limit = DefaultSessionPoolSizeLimit
		}

		if p.IdleThreshold == 0 {
			p.IdleThreshold = DefaultSessionPoolIdleThreshold
		}
		if p.IdleThreshold > 0 {
			p.keeperStop = make(chan struct{})
			p.keeperDone = make(chan struct{})
			go p.keeper()
		}

		if p.KeepAliveMinSize < 0 {
			p.KeepAliveMinSize = 0
		} else if p.KeepAliveMinSize == 0 {
			p.KeepAliveMinSize = DefaultKeepAliveMinSize
		}

		if p.IdleKeepAliveThreshold == 0 {
			p.IdleKeepAliveThreshold = DefaultIdleKeepAliveThreshold
		}

		if p.BusyCheckInterval == 0 {
			p.BusyCheckInterval = DefaultSessionPoolBusyCheckInterval
		}
		if p.BusyCheckInterval > 0 {
			p.busyCheckerStop = make(chan struct{})
			p.busyCheckerDone = make(chan struct{})
			// NOTE: if we make this buffered we also must cork it inside
			// Close().
			p.busyCheck = make(chan *Session)
			go p.busyChecker()
		}

		if p.CreateSessionTimeout <= 0 {
			p.CreateSessionTimeout = DefaultSessionPoolCreateSessionTimeout
		}
		if p.DeleteTimeout <= 0 {
			p.DeleteTimeout = DefaultSessionPoolDeleteTimeout
		}
		if p.KeepAliveTimeout <= 0 {
			p.KeepAliveTimeout = DefaultSessionPoolKeepAliveTimeout
		}
	})
}

// p.mu must NOT be held.
func (p *SessionPool) createSession(ctx context.Context) (*Session, error) {
	// pre-check the pool size
	p.mu.Lock()
	var s *Session
	p.createInProgress++
	enoughSpace := p.createInProgress+len(p.index) <= p.limit
	defer func() {
		p.createInProgress--
		p.mu.Unlock()
		if !enoughSpace && s != nil {
			p.closeSession(ctx, s)
		}
	}()

	if !enoughSpace {
		return nil, ErrSessionPoolOverflow
	}

	p.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, p.CreateSessionTimeout)
	defer cancel()
	s, err := p.Builder.CreateSession(ctx)
	p.mu.Lock()
	// while creating session pool may become full
	enoughSpace = p.createInProgress+len(p.index) <= p.limit

	if err != nil {
		return nil, err
	}
	if !enoughSpace {
		return nil, ErrSessionPoolOverflow
	}

	p.index[s] = sessionInfo{}
	s.OnClose(func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		info, has := p.index[s]
		if !has {
			return
		}

		delete(p.index, s)
		p.notify(nil)

		if info.idle != nil {
			panic("ydb: table: session closed while still in idle pool")
		}
		if info.ready != nil {
			p.ready.Remove(info.ready)
			info.ready = nil
		}
	})
	return s, nil
}

func isCreateSessionErrorRetriable(err error) bool {
	switch {
	case
		errors.Is(err, ErrSessionPoolOverflow),
		ydb.IsOpError(err, ydb.StatusOverloaded),
		ydb.IsTransportError(err, ydb.TransportErrorResourceExhausted),
		ydb.IsTransportError(err, ydb.TransportErrorDeadlineExceeded),
		ydb.IsTransportError(err, ydb.TransportErrorUnavailable):
		return true
	default:
		return false
	}
}

// Get returns first idle session from the SessionPool and removes it from
// there. If no items stored in SessionPool it creates new one by calling
// Builder.CreateSession() method and returns it.
func (p *SessionPool) Get(ctx context.Context) (s *Session, err error) {
	p.init()

	p.traceGetStart(ctx)
	defer func() {
		p.traceGetDone(ctx, s, err)
	}()

	const maxAttempts = 100
	for i := 0; s == nil && err == nil && i < maxAttempts; i++ {
		var (
			ch *chan *Session
			el *list.Element // Element in the wait queue.
		)
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrSessionPoolClosed
		}
		s = p.removeFirstIdle()
		p.mu.Unlock()

		if s == nil {
			// Try create new session without awaiting for reused one.
			s, err = p.createSession(ctx)

			if s != nil || (err != nil && !isCreateSessionErrorRetriable(err)) {
				return s, err
			}
			err = nil
		}

		// get here after ErrSessionPoolOverflow error
		if s == nil {
			// Try to wait for a touched session - pool is full.
			//
			// This should be done only if number of currently waiting goroutines
			// are less than maximum amount of touched session. That is, we want to
			// be fair here and not to lock more goroutines than we could ship
			// session to.
			p.mu.Lock()
			s = p.removeFirstIdle()
			if s != nil {
				p.mu.Unlock()
				return s, nil
			}
			ch = p.getWaitCh()
			el = p.waitq.PushBack(ch)
			p.mu.Unlock()
		}

		if ch == nil {
			continue
		}
		p.traceWaitStart(ctx)
		var ok bool
		select {
		case s, ok = <-*ch:
			// Note that race may occur and some goroutine may try to write
			// session into channel after it was enqueued but before it being
			// read here. In that case we will receive nil here and will retry.
			//
			// The same path will work when some session become deleted - the
			// nil value will be sent into the channel.
			if ok {
				// Put only filled and not closed channel back to the pool.
				// That is, we need to avoid races on filling reused channel
				// for the next waiter – session could be lost for a long time.
				p.putWaitCh(ch)
			}

		case <-ctx.Done():
			p.mu.Lock()
			// Note that el can be already removed here while we were moving
			// from reading from ch to this case. This does not make any
			// difference – channel will be closed by notifying goroutine.
			p.waitq.Remove(el)
			p.mu.Unlock()
			err = ctx.Err()
		}
		p.traceWaitDone(ctx, s, err)
	}
	if s == nil && err == nil {
		err = ErrNoProgress
	}

	return s, err
}

// Put returns session to the SessionPool for further reuse.
// If pool is already closed Put() calls s.Close(ctx) and returns
// ErrSessionPoolClosed.
//
// Note that Put() must be called only once after being created or received by
// Get() or Take() calls. In other way it will produce unexpected behavior or
// panic.
func (p *SessionPool) Put(ctx context.Context, s *Session) (err error) {
	p.init()

	p.tracePutStart(ctx, s)
	defer func() {
		p.tracePutDone(ctx, s, err)
	}()

	p.mu.Lock()
	switch {
	case p.closed:
		err = ErrSessionPoolClosed

	case p.idle.Len() >= p.limit:
		panicLocked(&p.mu, "ydb: table: Put() on full session pool")

	default:
		if !p.notify(s) {
			p.pushIdle(s, timeutil.Now())
		}
	}
	p.mu.Unlock()

	if err != nil {
		p.closeSession(ctx, s)
	}

	return
}

// PutBusy returns given session s into the pool after some operation on s was
// canceled by the client (probably due the timeout) or after some transport
// error received. That is, session may be still in request processing state
// and is not able to process further requests.
//
// Given session may be reused or may be closed in the future. That is, calling
// PutBusy() gives complete ownership of s to the pool.
func (p *SessionPool) PutBusy(ctx context.Context, s *Session) (err error) {
	p.init()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrSessionPoolClosed
	}
	info, has := p.index[s]
	if !has {
		panicLocked(&p.mu, "ydb: table: PutBusy() unknown session")
	}
	if info.idle != nil {
		panicLocked(&p.mu, "ydb: table: PutBusy() idle session")
	}
	if p.busyCheck == nil {
		panicLocked(&p.mu, "ydb: table: PutBusy() session into the pool without busy checker")
	}
	delete(p.index, s)
	p.notify(nil)
	p.mu.Unlock()

	select {
	case p.busyCheck <- s:

	case <-p.busyCheckerDone:
		p.closeSession(ctx, s)

	case <-ctx.Done():
		err = ctx.Err()
	}

	return
}

// Take removes session s from the pool and ensures that s will not be returned
// by other Take() or Get() calls.
//
// The intended way of Take() use is to create session by calling Create() and
// Put() it later to prepare KeepAlive tracking when session is idle. When
// Session becomes active, one should call Take() to stop KeepAlive tracking
// (simultaneous use of Session is prohibited).
//
// After session returned to the pool by calling PutBusy() it can not be taken
// by Take() any more. That is, semantically PutBusy() is the same as session's
// Close().
//
// It is assumed that Take() callers never call Get() method.
func (p *SessionPool) Take(ctx context.Context, s *Session) (took bool, err error) {
	p.init()

	p.traceTakeStart(ctx, s)
	defer func() {
		p.traceTakeDone(ctx, s, took, err)
	}()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false, ErrSessionPoolClosed
	}
	var has bool
	for has, took = p.takeIdle(s); has && !took && p.touching; has, took = p.takeIdle(s) {
		cond := p.touchCond()
		p.mu.Unlock()

		p.traceTakeWait(ctx, s)

		// Keepalive processing takes place right now.
		// Try to await touched session before creation of new one.
		select {
		case <-cond:

		case <-ctx.Done():
			return false, ctx.Err()
		}

		p.mu.Lock()
	}
	p.mu.Unlock()

	if !has {
		err = ErrSessionUnknown
	}

	return took, err
}

// Create creates new session and returns it.
// The intended way of Create() usage relates to Take() method.
func (p *SessionPool) Create(ctx context.Context) (s *Session, err error) {
	p.init()

	const maxAttempts = 10
	for i := 0; i < maxAttempts; i++ {
		p.mu.Lock()
		// NOTE: here is a race condition with keeper() running.
		// Session could be deleted by some reason after we released the mutex.
		// We are not dealing with this because even if session is not deleted
		// by keeper() it could be staled on the server and the same user
		// experience will appear.
		s = p.getReady()
		p.mu.Unlock()

		if s == nil {
			return p.createSession(ctx)
		}

		took, err := p.Take(ctx, s)
		if err == nil && !took || err == ErrSessionUnknown {
			// Session was marked for deletion or deleted by keeper() - race happen - retry
			s = nil
			err = nil
			continue
		}
		if err != nil {
			p.mu.Lock()
			if !p.closed {
				p.pushReady(s)
			}
			p.mu.Unlock()
			return nil, err
		}

		return s, nil
	}

	return nil, ErrNoProgress
}

// Close deletes all stored sessions inside SessionPool.
// It also stops all underlying timers and goroutines.
// It returns first error occured during stale sessions deletion.
// Note that even on error it calls Close() on each session.
func (p *SessionPool) Close(ctx context.Context) (err error) {
	p.init()

	p.traceCloseStart(ctx)
	defer func() {
		p.traceCloseDone(ctx, err)
	}()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	keeperDone := p.keeperDone
	if ch := p.keeperStop; ch != nil {
		close(ch)
	}

	busyCheckerDone := p.busyCheckerDone
	if ch := p.busyCheckerStop; ch != nil {
		close(ch)
	}
	p.mu.Unlock()

	if keeperDone != nil {
		<-keeperDone
	}
	if busyCheckerDone != nil {
		<-busyCheckerDone
	}

	p.mu.Lock()
	idle := p.idle
	waitq := p.waitq
	p.limit = 0
	p.idle = list.New()
	p.ready = list.New()
	p.waitq = list.New()
	p.index = make(map[*Session]sessionInfo)
	p.mu.Unlock()

	for el := waitq.Front(); el != nil; el = el.Next() {
		ch := el.Value.(*chan *Session)
		close(*ch)
	}
	for e := idle.Front(); e != nil; e = e.Next() {
		s := e.Value.(*Session)
		p.closeSession(ctx, s)
	}

	return nil
}

func (p *SessionPool) Stats() SessionPoolStats {
	idleCount := 0
	if p.idle != nil {
		idleCount = p.idle.Len()
	}
	readyCount := 0
	if p.ready != nil {
		readyCount = p.ready.Len()
	}

	return SessionPoolStats{
		Idle:    idleCount,
		Ready:   readyCount,
		MinSize: p.KeepAliveMinSize,
		MaxSize: p.SizeLimit,
	}
}

func (p *SessionPool) busyChecker() {
	defer close(p.busyCheckerDone)
	var (
		toCheck []*Session

		active = false
		timer  = timeutil.NewStoppedTimer()
		ctx    = context.Background()
	)
	for {
		select {
		case <-p.busyCheckerStop:
			for _, s := range toCheck {
				p.closeSession(ctx, s)
			}
			return

		case s := <-p.busyCheck:
			p.traceBusyCheckStart(ctx, s)

			if len(toCheck) >= p.limit {
				// Do not check more sessions than pool's capacity.
				p.closeSession(ctx, s)
				p.traceBusyCheckDone(ctx, s, false, nil)
				continue
			}

			toCheck = append(toCheck, s)
			if !active {
				active = true
				timer.Reset(p.BusyCheckInterval)
			}

		case <-timer.C():
			active = false
			for i := 0; i < len(toCheck); {
				s := toCheck[i]
				info, err := p.keepAliveSession(ctx, s)
				if err != nil || info.Status == SessionReady {
					n := len(toCheck)
					toCheck[i] = toCheck[n-1]
					toCheck[n-1] = nil
					toCheck = toCheck[:n-1]

					p.mu.Lock()
					enoughSpace := !p.closed && len(p.index) < p.limit
					reuse := enoughSpace && err == nil
					if reuse {
						p.index[s] = sessionInfo{}
						if !p.notify(s) {
							p.pushIdle(s, timeutil.Now())
							p.pushReady(s)
						}
					}
					p.mu.Unlock()
					if !reuse {
						p.closeSession(ctx, s)
					}

					p.traceBusyCheckDone(ctx, s, reuse, err)
				} else {
					i++
				}
			}
			if len(toCheck) > 0 {
				// Timer is never active here.
				timer.Reset(p.BusyCheckInterval)
			}
		}
	}
}

func (p *SessionPool) keeper() {
	defer close(p.keeperDone)
	var (
		toTouch    []*Session // Cached for reuse.
		toDelete   []*Session // Cached for reuse.
		toTryAgain []*Session // Cached for reuse.

		wake  = make(chan struct{})
		timer = timeutil.NewTimer(p.IdleThreshold)
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
			timer.Reset(p.IdleThreshold)
			continue

		case <-p.keeperStop:
			return

		case now = <-timer.C():
			toTouch = toTouch[:0]
			toTryAgain = toTryAgain[:0]
			toDelete = toDelete[:0]

			p.mu.Lock()
			{
				p.touching = true
				// Iterate over n most idle items.
				n := p.KeepAliveBatchSize
				if n <= 0 {
					n = p.idle.Len()
				}
				for i := 0; i < n; i++ {
					s, touched := p.peekFirstIdle()
					if s == nil || now.Sub(touched) < p.IdleThreshold {
						break
					}
					_ = p.removeIdle(s)
					toTouch = append(toTouch, s)
				}
			}
			p.mu.Unlock()

			var mark *list.Element // Element in the list to insert touched sessions after.
			for i, s := range toTouch {
				toTouch[i] = nil

				p.mu.Lock()
				keepAliveCount := p.incrementKeepAlive(s)
				p.mu.Unlock()
				// if keepAlive was called more than the corresponding limit for the session to be alive and more
				// sessions are open than the lower limit of continuously kept sessions
				if p.IdleKeepAliveThreshold > 0 && keepAliveCount > p.IdleKeepAliveThreshold &&
					p.KeepAliveMinSize < len(p.index) {

					toDelete = append(toDelete, s)
					continue
				}

				_, err := p.keepAliveSession(context.Background(), s)
				if err != nil {
					opErr, ok := err.(*ydb.OpError)
					if ok && opErr.Reason == ydb.StatusBadSession {
						toDelete = append(toDelete, s)
					} else {
						toTryAgain = append(toTryAgain, s)
					}
					continue
				}

				p.mu.Lock()
				if !p.notify(s) {
					// Need to push back session into list in order, to prevent
					// shuffling of sessions order.
					//
					// That is, there may be a race condition, when some session S1
					// pushed back in the list before we took the mutex. Suppose S1
					// touched time is greater than ours `now` for S0. If so, it
					// then may interrupt next keep alive iteration earlier and
					// prevent our session S0 being touched:
					// time.Since(S1) < threshold but time.Since(S0) > threshold.
					mark = p.pushIdleInOrderAfter(s, now, mark)
				}
				p.mu.Unlock()
			}

			{ // push all the soft failed sessions to retry on the next tick
				pushBackTime := now.Add(-p.IdleThreshold)

				p.mu.Lock()
				for _, el := range toTryAgain {
					_ = p.pushIdleInOrder(el, pushBackTime)
				}
				p.mu.Unlock()
			}

			var (
				sleep bool
				delay time.Duration
			)
			p.mu.Lock()

			if s, touched := p.peekFirstIdle(); s == nil {
				// No sessions to check. Let the Put() caller to wake up
				// keeper when session arrive.
				sleep = true
				p.keeperWake = wake
			} else {
				// NOTE: negative delay is also fine.
				delay = p.IdleThreshold - now.Sub(touched)
			}

			// Takers notification broadcast channel.
			touchingDone := p.touchingDone
			p.touchingDone = nil
			p.touching = false

			p.mu.Unlock()

			if !sleep {
				timer.Reset(delay)
			}
			for i, s := range toDelete {
				toDelete[i] = nil
				p.closeSession(context.Background(), s)
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
// sync.Pool.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (p *SessionPool) getWaitCh() *chan *Session {
	if p.testHookGetWaitCh != nil {
		p.testHookGetWaitCh()
	}
	s, ok := p.waitChPool.Get().(*chan *Session)
	if !ok {
		// NOTE: MUST NOT be buffered.
		// In other case we could cork an already no-owned channel.
		ch := make(chan *Session)
		s = &ch
	}
	return s
}

// putWaitCh receives pointer to a channel and makes it available for further
// use.
// Note that ch MUST NOT be owned by any goroutine at the call moment and ch
// MUST NOT contain any value.
func (p *SessionPool) putWaitCh(ch *chan *Session) {
	p.waitChPool.Put(ch)
}

// p.mu must be held.
func (p *SessionPool) peekFirstIdle() (s *Session, touched time.Time) {
	el := p.idle.Front()
	if el == nil {
		return
	}
	s = el.Value.(*Session)
	info, has := p.index[s]
	if !has || el != info.idle {
		panicLocked(&p.mu, "ydb: table: inconsistent session pool index")
	}
	return s, info.touched
}

// removes first Session from idle and resets the keepAliveCount
// to prevent session from dying in the keeper after it was returned
// to be used only in outgoing functions that make Session busy.
// p.mu must be held.
func (p *SessionPool) removeFirstIdle() *Session {
	s, _ := p.peekFirstIdle()
	if s != nil {
		info := p.removeIdle(s)
		info.keepAliveCount = 0
		p.index[s] = info
	}
	return s
}

// Increments the Keep Alive Counter and returns the previous number.
// Unlike other info modifiers, this one doesn't care if it didn't find the session, it skips
// the action. You can still check it later if needed, if the return code is -1
// p.mu must be held.
func (p *SessionPool) incrementKeepAlive(s *Session) int {
	info, has := p.index[s]
	if !has || info.idle == nil {
		return -1
	}
	ret := info.keepAliveCount
	info.keepAliveCount++
	p.index[s] = info
	return ret
}

// p.mu must be held.
func (p *SessionPool) touchCond() <-chan struct{} {
	if p.touchingDone == nil {
		p.touchingDone = make(chan struct{})
	}
	return p.touchingDone
}

// p.mu must be held.
func (p *SessionPool) notify(s *Session) (notified bool) {
	for el := p.waitq.Front(); el != nil; el = p.waitq.Front() {
		// Some goroutine is waiting for a session.
		//
		// It could be in this states:
		//   1) Reached the select code and awaiting for a value in channel.
		//   2) Reached the select code but already in branch of context
		//   cancelation. In this case it is locked on p.mu.Lock().
		//   3) Not reached the select code and thus not reading yet from the
		//   channel.
		//
		// For cases (2) and (3) we close the channel to signal that goroutine
		// missed something and may want to retry (especially for case (3)).
		//
		// After that we taking a next waiter and repeat the same.
		ch := p.waitq.Remove(el).(*chan *Session)
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

// p.mu must NOT be held.
func (p *SessionPool) closeSession(ctx context.Context, s *Session) {
	ctx, cancel := context.WithTimeout(ctx, p.DeleteTimeout)
	defer cancel()
	_ = s.Close(ctx)
}

func (p *SessionPool) keepAliveSession(ctx context.Context, s *Session) (SessionInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, p.KeepAliveTimeout)
	defer cancel()
	return s.KeepAlive(ctx)
}

// p.mu must be held.
func (p *SessionPool) removeIdle(s *Session) sessionInfo {
	info, has := p.index[s]
	if !has || info.idle == nil {
		panicLocked(&p.mu, "ydb: table: inconsistent session pool index")
	}

	p.idle.Remove(info.idle)
	info.idle = nil
	p.index[s] = info
	return info
}

// Removes Session from idle pool and resets keepAliveCount for it not
// to die in keeper when it will be returned
// to be used only in outgoing functions that make Session busy.
// p.mu must be held.
func (p *SessionPool) takeIdle(s *Session) (has, took bool) {
	var info sessionInfo
	info, has = p.index[s]
	if !has {
		// Could not be strict here and panic – session may become deleted by
		// keeper().
		return
	}
	if info.idle == nil {
		// Session s is not idle.
		return
	}
	took = true
	info = p.removeIdle(s)
	info.keepAliveCount = 0
	p.index[s] = info
	return
}

// p.mu must be held.
func (p *SessionPool) pushIdle(s *Session, now time.Time) {
	p.handlePushIdle(s, now, p.idle.PushBack(s))
}

// p.mu must be held.
func (p *SessionPool) pushIdleInOrder(s *Session, now time.Time) (el *list.Element) {
	var prev *list.Element
	for prev = p.idle.Back(); prev != nil; prev = prev.Prev() {
		s := prev.Value.(*Session)
		t := p.index[s].touched
		if !now.Before(t) { // now >= t
			break
		}
	}
	if prev != nil {
		el = p.idle.InsertAfter(s, prev)
	} else {
		el = p.idle.PushFront(s)
	}
	p.handlePushIdle(s, now, el)
	return el
}

// p.mu must be held.
func (p *SessionPool) pushIdleInOrderAfter(s *Session, now time.Time, mark *list.Element) *list.Element {
	if mark != nil {
		n := p.idle.Len()
		el := p.idle.InsertAfter(s, mark)
		if n < p.idle.Len() {
			// List changed, thus mark belongs to list.
			p.handlePushIdle(s, now, el)
			return el
		}
	}
	return p.pushIdleInOrder(s, now)
}

// p.mu must be held.
func (p *SessionPool) handlePushIdle(s *Session, now time.Time, el *list.Element) {
	info, has := p.index[s]
	if !has {
		panicLocked(&p.mu, "ydb: table: trying to store session created outside of the pool")
	}
	if info.idle != nil {
		panicLocked(&p.mu, "ydb: table: inconsistent session pool index")
	}

	info.touched = now
	info.idle = el
	p.index[s] = info

	p.wakeUpKeeper()
}

// p.mu must be held.
func (p *SessionPool) wakeUpKeeper() {
	if wake := p.keeperWake; wake != nil {
		p.keeperWake = nil
		close(wake)
	}
}

// p.mu must be held.
func (p *SessionPool) getReady() *Session {
	if p.ready.Len() == 0 {
		return nil
	}

	s := p.ready.Remove(p.ready.Front()).(*Session)
	info, has := p.index[s]
	if !has {
		panicLocked(&p.mu, "ydb: table: trying to store session created outside of the pool")
	}
	if info.ready == nil {
		panicLocked(&p.mu, "ydb: table: inconsistent session pool index")
	}
	info.ready = nil
	return s
}

// p.mu must be held.
func (p *SessionPool) pushReady(s *Session) {
	info, has := p.index[s]
	if !has {
		panicLocked(&p.mu, "ydb: table: trying to store session created outside of the pool")
	}
	if info.ready != nil {
		panicLocked(&p.mu, "ydb: table: inconsistent session pool index")
	}
	info.ready = p.ready.PushBack(s)
	p.index[s] = info
}

func (p *SessionPool) traceGetStart(ctx context.Context) {
	x := SessionPoolGetStartInfo{
		Context: ctx,
	}
	if a := p.Trace.GetStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).GetStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceGetDone(ctx context.Context, s *Session, err error) {
	x := SessionPoolGetDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := p.Trace.GetDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).GetDone; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceWaitStart(ctx context.Context) {
	x := SessionPoolWaitStartInfo{
		Context: ctx,
	}
	if a := p.Trace.WaitStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).WaitStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceWaitDone(ctx context.Context, s *Session, err error) {
	x := SessionPoolWaitDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := p.Trace.WaitDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).WaitDone; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceBusyCheckStart(ctx context.Context, s *Session) {
	x := SessionPoolBusyCheckStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := p.Trace.BusyCheckStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).BusyCheckStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceBusyCheckDone(ctx context.Context, s *Session, reused bool, err error) {
	x := SessionPoolBusyCheckDoneInfo{
		Context: ctx,
		Session: s,
		Reused:  reused,
		Error:   err,
	}
	if a := p.Trace.BusyCheckDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).BusyCheckDone; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceTakeStart(ctx context.Context, s *Session) {
	x := SessionPoolTakeStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := p.Trace.TakeStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).TakeStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceTakeWait(ctx context.Context, s *Session) {
	x := SessionPoolTakeWaitInfo{
		Context: ctx,
		Session: s,
	}
	if a := p.Trace.TakeWait; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).TakeWait; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceTakeDone(ctx context.Context, s *Session, took bool, err error) {
	x := SessionPoolTakeDoneInfo{
		Context: ctx,
		Session: s,
		Took:    took,
		Error:   err,
	}
	if a := p.Trace.TakeDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).TakeDone; b != nil {
		b(x)
	}
}
func (p *SessionPool) tracePutStart(ctx context.Context, s *Session) {
	x := SessionPoolPutStartInfo{
		Context: ctx,
		Session: s,
	}
	if a := p.Trace.PutStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).PutStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) tracePutDone(ctx context.Context, s *Session, err error) {
	x := SessionPoolPutDoneInfo{
		Context: ctx,
		Session: s,
		Error:   err,
	}
	if a := p.Trace.PutDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).PutDone; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceCloseStart(ctx context.Context) {
	x := SessionPoolCloseStartInfo{
		Context: ctx,
	}
	if a := p.Trace.CloseStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).CloseStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceCloseDone(ctx context.Context, err error) {
	x := SessionPoolCloseDoneInfo{
		Context: ctx,
		Error:   err,
	}
	if a := p.Trace.CloseDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).CloseDone; b != nil {
		b(x)
	}
}

type sessionInfo struct {
	idle           *list.Element
	ready          *list.Element
	touched        time.Time
	keepAliveCount int
}

func panicLocked(mu sync.Locker, message string) {
	mu.Unlock()
	panic(message)
}

type SessionPoolStats struct {
	Idle    int
	Ready   int
	MinSize int
	MaxSize int
}
