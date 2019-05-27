package table

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

var (
	DefaultSessionPoolKeepAliveTimeout = 500 * time.Millisecond
	DefaultSessionPoolDeleteTimeout    = 500 * time.Millisecond
	DefaultSessionPoolIdleThreshold    = 5 * time.Second
	DefaultSessionPoolSizeLimit        = 50
)

var (
	// ErrSessionPoolClosed is returned by a SessionPool instance to indicate
	// that pool is closed and not able to complete requested operation.
	ErrSessionPoolClosed = errors.New("ydb: table: session pool is closed")
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

	// IdleLimit is an upper bound of pooled sessions without any activity
	// within.
	//IdleLimit int

	// IdleThreshold is a maximum duration between any activity within session.
	// If this threshold reached, KeepAlive() method will be called on idle session.
	// If IdleThreshold is zero then there is no idle limit.
	IdleThreshold time.Duration

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

	// DeleteTimeout limits maximum time spent on Delete request for
	// KeepAliveBatchSize number of sessions.
	// If DeleteTimeout is less than or equal to zero then the
	// DefaultSessionPoolDeleteTimeout is used.
	DeleteTimeout time.Duration

	mu           sync.Mutex
	initOnce     sync.Once
	created      int        // Number of created sessions.
	limit        int        // Upper bound for pool size.
	idle         *list.List // list<*Session>
	waitq        *list.List // list<*chan *Session>
	index        map[*Session]sessionInfo
	wakeKeeper   chan struct{} // Set by keeper.
	stopKeeper   chan struct{}
	doneKeeper   chan struct{}
	doneTouching chan struct{}
	touching     bool
	closed       bool
}

func (p *SessionPool) init() {
	p.initOnce.Do(func() {
		p.idle = list.New()
		p.index = make(map[*Session]sessionInfo)

		p.limit = p.SizeLimit
		if p.limit <= 0 {
			p.limit = DefaultSessionPoolSizeLimit
		}
		p.waitq = list.New()

		if p.IdleThreshold != 0 {
			p.stopKeeper = make(chan struct{})
			p.doneKeeper = make(chan struct{})
			go p.keeper()
		}
	})
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
			return nil, ErrSessionPoolClosed
		}
		s = p.front()
		switch {
		case s == nil && p.created < p.limit:
			// Can create new session without awaiting for reused one.
			// Note that we must not increase p.n counter until successful session
			// creation – in other way we will block some getter awaing on reused
			// session which creation was actually failed here.
			//
			// But on the other side, this behavior is racy – there is a
			// probability of creation of multiple session here (for the first time
			// of pool usage, thus it is rare). We deal well with this race in the
			// Put() implementation.
			p.mu.Unlock()

			s, err = p.Builder.CreateSession(ctx)
			if err != nil {
				return
			}

			p.mu.Lock()
			if p.created == p.limit {
				p.mu.Unlock()
				// We lost the race – pool is full now and session can not be reused.
				p.closeSession(ctx, s)
				s = nil
				p.mu.Lock()
			} else {
				p.created++
				s.OnClose(func() {
					p.mu.Lock()
					if !p.closed {
						p.created--
						p.notify(nil)
					}
					p.mu.Unlock()
				})
			}
		case s == nil && p.created == p.limit:
			// Try to wait for a touched session instead of creating new one.
			//
			// This should be done only if number of currently waiting goroutines
			// are less than maximum amount of touched session. That is, we want to
			// be fair here and not to lock more goroutines than we could ship
			// session to.
			ch = getWaitCh()
			el = p.waitq.PushBack(ch)
		}
		p.mu.Unlock()

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
				putWaitCh(ch)
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
		panic("ydb: table: Put() on full session pool")

	default:
		if !p.notify(s) {
			p.pushBack(s, timeutil.Now())
		}
	}
	p.mu.Unlock()

	if err != nil {
		p.closeSession(ctx, s)
	}

	return
}

// Take removes session s from the pool and ensures that s will not be returned
// by other Take() or Get() calls.
//
// The intended way of Take() use is to create Session somehow from outside
// the SessionPool and put it in to prepare KeepAlive tracking when Session is
// idle. When Session becomes active, one should call Take() to stop KeepAlive
// tracking (simultaneous use of Session is prohibited).
func (p *SessionPool) Take(ctx context.Context, s *Session) (has bool, err error) {
	p.init()

	p.traceTakeStart(ctx, s)
	defer func() {
		p.traceTakeDone(ctx, s, has)
	}()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false, ErrSessionPoolClosed
	}
	for has = p.take(s); !has && p.touching; has = p.take(s) {
		cond := p.touchCond()
		p.mu.Unlock()

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

	return has, nil
}

// Reset deletes all stored sessions inside SessionPool.
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

	doneKeeper := p.doneKeeper
	if p.stopKeeper != nil {
		close(p.stopKeeper)
	}
	p.mu.Unlock()

	if doneKeeper != nil {
		<-doneKeeper
	}

	p.mu.Lock()
	idle := p.idle
	waitq := p.waitq
	p.stopKeeper = nil
	p.doneKeeper = nil
	p.wakeKeeper = nil
	p.index = nil
	p.idle = nil
	p.waitq = nil
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

func (p *SessionPool) keeper() {
	defer close(p.doneKeeper)
	var (
		toTouch  []*Session // Cached for reuse.
		toDelete []*Session // Cached for reuse.

		wake  = make(chan struct{})
		timer = timeutil.NewTimer(p.IdleThreshold)
	)

	touchTimeout := p.KeepAliveTimeout
	if touchTimeout <= 0 {
		touchTimeout = DefaultSessionPoolKeepAliveTimeout
	}

	for {
		var now time.Time
		select {
		case now = <-timer.C():
			// Handle tick outside select.
			toTouch = toTouch[:0]
			toDelete = toDelete[:0]

		case <-wake:
			wake = make(chan struct{})
			timer.Reset(p.IdleThreshold)
			continue

		case <-p.stopKeeper:
			return
		}

		p.mu.Lock()
		{
			p.touching = true
			// Iterate over n most idle items.
			n := p.KeepAliveBatchSize
			if n <= 0 {
				n = p.idle.Len()
			}
			for i := 0; i < n; i++ {
				s, el, touched := p.peekFront()
				if s == nil || now.Sub(touched) < p.IdleThreshold {
					break
				}
				p.remove(s, el)
				toTouch = append(toTouch, s)
			}
		}
		p.mu.Unlock()

		var mark *list.Element // Element in the list to insert touched sessions after.
		for i, s := range toTouch {
			toTouch[i] = nil

			ctx, cancel := contextTimeout(touchTimeout)
			err := s.KeepAlive(ctx)
			cancel()
			if err != nil {
				toDelete = append(toDelete, s)
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
				mark = p.pushBackInOrderAfter(s, now, mark)
			}
			p.mu.Unlock()
		}

		var (
			sleep bool
			delay time.Duration
		)
		p.mu.Lock()

		if s, _, touched := p.peekFront(); s == nil {
			// No sessions to check. Let the Put() caller to wake up
			// keeper when session arrive.
			sleep = true
			p.wakeKeeper = wake
		} else {
			// NOTE: negative delay is also fine.
			delay = p.IdleThreshold - now.Sub(touched)
		}

		// Takers notification broadcast channel.
		doneTouching := p.doneTouching
		p.doneTouching = nil
		p.touching = false

		p.mu.Unlock()

		if doneTouching != nil {
			close(doneTouching)
		}
		if !sleep {
			timer.Reset(delay)
		}
		for i, s := range toDelete {
			toDelete[i] = nil
			p.closeSession(context.Background(), s)
		}
	}
}

var (
	waitChPool        sync.Pool
	testHookGetWaitCh func() // nil except some tests.
)

// getWaitCh returns pointer to a channel of sessions.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Pool.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func getWaitCh() *chan *Session {
	if testHookGetWaitCh != nil {
		testHookGetWaitCh()
	}
	p, ok := waitChPool.Get().(*chan *Session)
	if !ok {
		// NOTE: MUST NOT be buffered.
		// In other case we could cork an already no-owned channel.
		ch := make(chan *Session)
		p = &ch
	}
	return p
}

// putWaitCh receives pointer to a channel and makes it available for further
// use.
// Note that ch MUST NOT be owned by any goroutine at the call moment and ch
// MUST NOT contain any value.
func putWaitCh(ch *chan *Session) {
	waitChPool.Put(ch)
}

// p.mu must be held.
func (p *SessionPool) peekFront() (*Session, *list.Element, time.Time) {
	if p.idle == nil {
		return nil, nil, time.Time{}
	}
	el := p.idle.Front()
	if el == nil {
		return nil, nil, time.Time{}
	}
	s := el.Value.(*Session)
	x := p.index[s]
	return s, x.element, x.touched
}

// p.mu must be held.
func (p *SessionPool) front() *Session {
	s, el, _ := p.peekFront()
	if s != nil {
		p.remove(s, el)
	}
	return s
}

// p.mu must be held.
func (p *SessionPool) touchCond() <-chan struct{} {
	if p.doneTouching == nil {
		p.doneTouching = make(chan struct{})
	}
	return p.doneTouching
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

func (p *SessionPool) closeSession(ctx context.Context, s *Session) {
	deleteTimeout := p.DeleteTimeout
	if deleteTimeout <= 0 {
		deleteTimeout = DefaultSessionPoolDeleteTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()
	s.Close(ctx)
}

// p.mu must be held.
func (p *SessionPool) remove(s *Session, el *list.Element) {
	delete(p.index, s)
	p.idle.Remove(el)
}

// p.mu must be held.
func (p *SessionPool) take(s *Session) (ok bool) {
	if x, ok := p.index[s]; ok {
		p.remove(s, x.element)
		return true
	}
	return false
}

// p.mu must be held.
func (p *SessionPool) pushBack(s *Session, now time.Time) (el *list.Element) {
	el = p.idle.PushBack(s)
	p.handlePush(s, now, el)
	return el
}

// p.mu must be held.
func (p *SessionPool) pushBackInOrder(s *Session, now time.Time) (el *list.Element) {
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
	p.handlePush(s, now, el)
	return el
}

// p.mu must be held.
func (p *SessionPool) pushBackInOrderAfter(s *Session, now time.Time, mark *list.Element) *list.Element {
	if mark != nil {
		n := p.idle.Len()
		el := p.idle.InsertAfter(s, mark)
		if n < p.idle.Len() {
			// List changed, thus mark belongs to list.
			p.handlePush(s, now, el)
			return el
		}
	}
	return p.pushBackInOrder(s, now)
}

// p.mu must be held.
func (p *SessionPool) handlePush(s *Session, now time.Time, el *list.Element) {
	if _, has := p.index[s]; has {
		panic("ydb: table: trying to store already present session")
	}
	p.index[s] = sessionInfo{
		element: el,
		touched: now,
	}
	if wake := p.wakeKeeper; wake != nil {
		p.wakeKeeper = nil
		close(wake)
	}
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
func (p *SessionPool) traceTakeDone(ctx context.Context, s *Session, took bool) {
	x := SessionPoolTakeDoneInfo{
		Context: ctx,
		Session: s,
		Took:    took,
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

var noopCancel = func() {}

func contextTimeout(t time.Duration) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if t < 0 {
		return ctx, noopCancel
	}
	return context.WithTimeout(ctx, t)
}

type sessionInfo struct {
	element *list.Element
	touched time.Time
}
