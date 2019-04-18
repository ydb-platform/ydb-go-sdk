package table

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
)

var (
	DefaultSessionPoolKeepAliveTimeout   = 500 * time.Millisecond
	DefaultSessionPoolDeleteTimeout      = 500 * time.Millisecond
	DefaultSessionPoolIdleThreshold      = 5 * time.Second
	DefaultSessionPoolKeepAliveBatchSize = 50
)

// SessionBuilder is the interface that holds logic of creating or deleting
// sessions.
type SessionBuilder interface {
	CreateSession(context.Context) (*Session, error)
}

// SessionPool is a set of Session instances that may be reused.
// A SessionPool is safe for use by multiple goroutines simultaneously.
type SessionPool struct {
	// Builder holds an object capable for creating and deleting sessions.
	// It must not be nil.
	Builder SessionBuilder

	// SizeLimit is an upper bound of pooled sessions.
	// If SizeLimit is zero then no sessions will be reused. That is, with zero
	// SizeLimit SessionPool becomes noop proxy for Builder methods.
	// If SizeLimit is negative, then there is no size limit.
	SizeLimit int

	// Trace is an optional session lifetime tracing options.
	Trace SessionPoolTrace

	// IdleThreshold is a maximum duration between any activity within session.
	// If this threshold reached, KeepAlive() method will be called on idle session.
	// If IdleThreshold is zero then there is no idle limit.
	IdleThreshold time.Duration

	// If KeepAliveBatchSize is zero then the default value of
	// DefaultSessionPoolKeepAliveBatchSize session per tick is used.
	// If KeepAliveBatchSize is negative, then there is no batch limit. That
	// is, any number of session may be touched per single tick.
	KeepAliveBatchSize int

	// KeepAliveTimeout limits maximum time spent on KeepAlive request for
	// KeepAliveBatchSize number of sessions.
	// If KeepAliveTimeout is zero then the DefaultSessionPoolKeepAliveTimeout
	// is used.
	// If KeepAliveTimeout is negative, then no timeout is used. Note that this
	// may lead to deadlocks if Get() or Take() methods called with background
	// or non-cancelable contexts.
	KeepAliveTimeout time.Duration

	// DeleteTimeout limits maximum time spent on Delete request for
	// KeepAliveBatchSize number of sessions.
	// If DeleteTimeout is zero then the DefaultSessionPoolDeleteTimeout is
	// used.
	// If DeleteTimeout is negative, then no timeout is used. Note that this
	// may lead to deadlocks if Get() or Take() methods called with background
	// or non-cancelable contexts.
	DeleteTimeout time.Duration

	mu         sync.Mutex
	idle       *list.List // list<*Session>
	waitq      *list.List // list<*chan *Session>
	waitn      int        // Current number of waiters (even removed from waitq).
	waitm      int        // Maximum number of waiters in the waitq.
	index      map[*Session]sessionInfo
	stopKeeper chan struct{}
	doneKeeper chan struct{}
	wakeKeeper chan struct{} // Set by keeper.
	touching   bool
	stopping   bool

	doneTouching chan struct{}
}

// p.mu must be held.
func (p *SessionPool) init() {
	if p.idle == nil {
		p.idle = list.New()
		p.index = make(map[*Session]sessionInfo)

		p.waitq = list.New()
		p.waitm = p.KeepAliveBatchSize
		if p.waitm <= 0 {
			p.waitm = DefaultSessionPoolKeepAliveBatchSize
		}

		if p.IdleThreshold != 0 {
			p.stopKeeper = make(chan struct{})
			p.doneKeeper = make(chan struct{})
			// TODO: keeper mutates many variables even after stop.
			go p.keeper()
		}
	}
}

// Get returns first idle session from the SessionPool and removes it from
// there. If no items stored in SessionPool it creates new one by calling
// Builder.CreateSession() method and returns it.
func (p *SessionPool) Get(ctx context.Context) (s *Session, err error) {
	p.traceGetStart(ctx)
	defer func() {
		p.traceGetDone(ctx, s, err)
	}()

	var ch *chan *Session
	p.mu.Lock()
	s = p.front()
	if s == nil && p.touching && p.waitn < p.waitm {
		// Try to wait for a touched session instead of creating new one.
		//
		// This should be done only if number of currently waiting goroutines
		// are less than maximum amount of touched session. That is, we want to
		// be fair here and not to lock more goroutines than we could ship
		// session to.
		ch = getWaitCh()
		p.waitq.PushBack(ch)
		p.waitn++
	}
	p.mu.Unlock()
	if ch != nil {
		select {
		case s = <-*ch:
			// Put only filled channel to pool. That is, we need to avoid races
			// on filling reused channel for the next waiter. That is, someone
			// may take this channel in further Get() and receive our session.
			// This seems okay but breaks wait order and is not fair.
			putWaitCh(ch)

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if s != nil {
		return s, nil
	}
	// Out of luck. Probably someone else received touched session or no
	// session touched at all. Create new session.
	return p.Builder.CreateSession(ctx)
}

// Put returns session to the SessionPool for further reuse.
// If s can not be reused, Put() calls s.Delete(ctx) and returns the result.
//
// Note that Put() must be called only once after being created or received by
// Get() or Take() calls. In other way it will produce unexpected behavior or
// panic.
func (p *SessionPool) Put(ctx context.Context, s *Session) (reused bool, err error) {
	p.tracePutStart(ctx, s)
	defer func() {
		p.tracePutDone(ctx, s, err)
	}()
	p.mu.Lock()
	p.init()
	if p.SizeLimit < 0 || p.idle.Len() < p.SizeLimit {
		p.pushBack(s, timeutil.Now())
		reused = true
	}
	p.mu.Unlock()
	if !reused {
		err = s.Delete(ctx)
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
	p.traceTakeStart(ctx, s)
	defer func() {
		p.traceTakeDone(ctx, s, has)
	}()

	p.mu.Lock()
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
// Note that even on error it calls Delete() on each session.
func (p *SessionPool) Reset(ctx context.Context) (err error) {
	p.traceResetStart(ctx)
	defer func() {
		p.traceResetDone(ctx, err)
	}()

	p.mu.Lock()
	idle := p.idle
	waitq := p.waitq
	if idle == nil || p.stopping {
		p.mu.Unlock()
		return
	}
	p.stopping = true

	doneKeeper := p.doneKeeper
	if p.stopKeeper != nil {
		close(p.stopKeeper)
	}
	p.mu.Unlock()

	if doneKeeper != nil {
		<-doneKeeper
	}

	p.mu.Lock()
	p.stopping = false
	p.stopKeeper = nil
	p.doneKeeper = nil
	p.wakeKeeper = nil
	p.index = nil
	p.idle = nil
	p.waitq = nil
	p.mu.Unlock()

	for el := waitq.Front(); el != nil; el = el.Next() {
		if ch, ok := el.Value.(*chan *Session); ok {
			select {
			case *ch <- nil:
			default:
			}
		}
	}

	var firstErr error
	for e := idle.Front(); e != nil; e = e.Next() {
		err := e.Value.(*Session).Delete(ctx)
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return firstErr
}

var noopCancel = func() {}

func contextTimeout(t time.Duration) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	if t < 0 {
		return ctx, noopCancel
	}
	return context.WithTimeout(ctx, t)
}

func (p *SessionPool) keeper() {
	defer close(p.doneKeeper)
	var (
		toTouch  []*Session // Cached for reuse.
		toDelete []*Session // Cached for reuse.

		wake chan struct{}

		timer = timeutil.NewTimer(p.IdleThreshold)
	)

	touchTimeout := p.KeepAliveTimeout
	if touchTimeout == 0 {
		touchTimeout = DefaultSessionPoolKeepAliveTimeout
	}
	deleteTimeout := p.DeleteTimeout
	if deleteTimeout == 0 {
		deleteTimeout = DefaultSessionPoolDeleteTimeout
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
			n := batchLimit(p.KeepAliveBatchSize, p.idle.Len()) // Iterate over n most idle items.
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

		keepctx, cancel := contextTimeout(touchTimeout)
		var (
			mark *list.Element // Element in the list to insert touched sessions after.
		)
		for i, s := range toTouch {
			toTouch[i] = nil
			if err := s.KeepAlive(keepctx); err != nil {
				toDelete = append(toDelete, s)
				continue
			}
			p.mu.Lock()
			select {
			case p.waitqRemoveFront() <- s:
				// Note that we do not change p.waitn here because it prevents
				// waiters overflow – we do not want to block more waiters than
				// sessions we could touch per one iteration.
			default:
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
		cancel()

		var (
			sleep bool
			delay time.Duration
		)
		p.mu.Lock()

		// Drain waitq. Notify remaining waiters with nil session to signal
		// that touching is done.
		for {
			ch := p.waitqRemoveFront()
			if ch == nil {
				break
			}
			select {
			case ch <- nil:
			default:
			}
		}

		p.touching = false

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

		p.mu.Unlock()

		if doneTouching != nil {
			close(doneTouching)
		}
		if !sleep {
			timer.Reset(delay)
		}
		if len(toDelete) == 0 {
			continue
		}
		delctx, cancel := contextTimeout(deleteTimeout)
		for _, s := range toDelete {
			s.Delete(delctx)
		}
		cancel()
	}
}

var waitChPool sync.Pool

// getWaitCh returns pointer to a channel of sessions.
// Note that returning a pointer reduces allocations on sync.Pool usage.
func getWaitCh() *chan *Session {
	ch, ok := waitChPool.Get().(chan *Session)
	if !ok {
		// NOTE: MUST NOT be buffered.
		// In other case we could cork an already no-owned channel.
		ch = make(chan *Session)
	}
	return &ch
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
func (p *SessionPool) waitqRemoveFront() chan<- *Session {
	el := p.waitq.Front()
	if el == nil {
		return nil
	}
	pch := p.waitq.Remove(el).(*chan *Session)
	p.waitn--
	return *pch
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
		panic("ydb: table: trying to store already present in pool session")
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
func (p *SessionPool) traceResetStart(ctx context.Context) {
	x := SessionPoolResetStartInfo{
		Context: ctx,
	}
	if a := p.Trace.ResetStart; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).ResetStart; b != nil {
		b(x)
	}
}
func (p *SessionPool) traceResetDone(ctx context.Context, err error) {
	x := SessionPoolResetDoneInfo{
		Context: ctx,
		Error:   err,
	}
	if a := p.Trace.ResetDone; a != nil {
		a(x)
	}
	if b := ContextSessionPoolTrace(ctx).ResetDone; b != nil {
		b(x)
	}
}

type sessionInfo struct {
	element *list.Element
	touched time.Time
}

func batchLimit(n, total int) int {
	if n == 0 {
		return 1
	}
	if n < 0 {
		return total
	}
	return n
}
