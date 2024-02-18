package pool

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errClosedPool   = xerrors.Wrap(errors.New("pool client closed early"))
	errPoolOverflow = xerrors.Wrap(errors.New("pool overflow"))
	errNoProgress   = xerrors.Wrap(errors.New("no progress"))
	errNilPool      = xerrors.Wrap(errors.New("pool is not initialized"))
)

type Pool[T any] interface {
	Close(ctx context.Context) error
	With(ctx context.Context, f func(ctx context.Context, s *T) error) (err error)

	// private API
	get(ctx context.Context) (s *T, err error)
	put(el *T) error
}

type pool[T any] struct {
	mu           xsync.Mutex
	idle         *list.List // list<*session>
	waitQ        *list.List // list<*chan *session>
	limit        int
	waitChPool   sync.Pool
	done         chan struct{}
	wg           sync.WaitGroup
	create       func(ctx context.Context) (*T, error)
	close        func(ctx context.Context, s *T) error
	deleteTimout time.Duration
}

func NewPool[T any](
	limit int,
	createEntity func(ctx context.Context) (*T, error),
	closeEntity func(ctx context.Context, s *T) error,
	deleteTimout time.Duration,
) Pool[T] {
	return &pool[T]{
		limit: limit,
		waitChPool: sync.Pool{
			New: func() interface{} {
				ch := make(chan *T)

				return &ch
			},
		},
		create:       createEntity,
		close:        closeEntity,
		idle:         list.New(),
		waitQ:        list.New(),
		deleteTimout: deleteTimout,
	}
}

func (p *pool[T]) With(ctx context.Context, f func(ctx context.Context, s *T) error) (err error) {
	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	default:

		s, err := p.get(ctx)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil {
				p.internalPoolSyncCloseSession(ctx, s)
			}
		}()

		err = f(ctx, s)
		if err != nil {
			return err
		}

		err = p.put(s)
		if err != nil {
			return err
		}

		return nil
	}
}

// checked
func (p *pool[T]) Close(ctx context.Context) error {
	if p == nil {
		return xerrors.WithStackTrace(errNilPool)
	}

	p.mu.WithLock(func() {
		select {
		case <-p.done:
			return
		default:
			close(p.done)

			p.limit = 0

			for el := p.waitQ.Front(); el != nil; el = el.Next() {
				ch := el.Value.(*chan *T)
				close(*ch)
			}

			for e := p.idle.Front(); e != nil; e = e.Next() {
				s := e.Value.(*T)
				// TODO don't forget do it in session
				// s.SetStatus(table.SessionClosing)
				p.wg.Add(1)
				go func() {
					defer p.wg.Done()
					p.internalPoolSyncCloseSession(ctx, s)
				}()
			}
		}
	})

	p.wg.Wait()

	return nil
}

// checked
func (p *pool[T]) internalPoolSyncCloseSession(ctx context.Context, s *T) {
	var cancel context.CancelFunc
	ctx, cancel = xcontext.WithTimeout(ctx, p.deleteTimout)
	defer cancel()

	_ = p.close(ctx, s)
}

// checked
// private API
func (p *pool[T]) get(ctx context.Context) (s *T, err error) {
	if p.isClosed() {
		return nil, xerrors.WithStackTrace(errClosedPool)
	}

	var (
		start = time.Now()
		i     = 0
	)

	const maxAttempts = 100
	for s == nil && err == nil && i < maxAttempts && !p.isClosed() {
		i++
		// First, we try to internalPoolGet session from idle
		p.mu.WithLock(func() {
			s = p.internalPoolRemoveFirstIdle()
		})

		if s != nil {
			return s, nil
		}

		// Second, we try to create new session
		s, err = p.create(ctx)
		if s == nil && err == nil {
			if err = ctx.Err(); err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			panic("both of session and err are nil")
		}

		if s != nil {
			return s, err
		}

		// Third, we try to wait for a touched session - Client is full.
		//
		// This should be done only if number of currently waiting goroutines
		// are less than maximum amount of touched session. That is, we want to
		// be fair here and not to lock more goroutines than we could ship
		// session to.
		s, err = p.internalPoolWaitFromCh(ctx)
		if err != nil {
			err = xerrors.WithStackTrace(err)
		}
	}

	if s == nil && err == nil {
		if p.isClosed() {
			err = xerrors.WithStackTrace(errClosedPool)
		} else {
			err = xerrors.WithStackTrace(errNoProgress)
		}
	}
	if err != nil {
		var idle int

		p.mu.WithLock(func() {
			idle = p.idle.Len()
		})

		return s, xerrors.WithStackTrace(
			fmt.Errorf("failed to get session from pool ("+
				"attempts: %d, latency: %v, pool have ( %d idle): %w",
				i, time.Since(start), idle, err,
			),
		)
	}

	return s, nil
}

// checked
// internalPoolGetWaitCh returns pointer to a channel of sessions.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Client.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (p *pool[T]) internalPoolGetWaitCh() *chan *T { //nolint:gocritic
	ch := p.waitChPool.Get()
	s, ok := ch.(*chan *T)
	if !ok {
		panic(fmt.Sprintf("%T is not a chan of sessions", ch))
	}

	return s
}

// checked
func (p *pool[T]) internalPoolWaitFromCh(ctx context.Context) (s *T, err error) {
	var (
		ch *chan *T
		el *list.Element // Element in the wait queue.
		ok bool
	)

	p.mu.WithLock(func() {
		ch = p.internalPoolGetWaitCh()
		el = p.waitQ.PushBack(ch)
	})

	select {
	case <-p.done:
		p.mu.WithLock(func() {
			p.waitQ.Remove(el)
		})
		return nil, xerrors.WithStackTrace(errClosedPool)

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
			p.waitChPool.Put(ch)
		}
		return s, nil

	case <-ctx.Done():
		p.mu.WithLock(func() {
			p.waitQ.Remove(el)
		})
		return nil, xerrors.WithStackTrace(ctx.Err())
	}
}

// checked
// removes first session from idle and resets the keepAliveCount
// to prevent session from dying in the internalPoolGC after it was returned
// to be used only in outgoing functions that make session busy.
// c.mu must be held.
func (p *pool[T]) internalPoolRemoveFirstIdle() *T {
	el := p.idle.Front()
	if el == nil {
		return nil
	}
	s := el.Value.(*T)
	if s != nil {
		p.idle.Remove(el)
	}
	return s
}

// checked
func (p *pool[T]) put(el *T) error {
	switch {
	case p.isClosed():
		return xerrors.WithStackTrace(errClosedPool)

	default:

		p.mu.Lock()
		defer p.mu.Unlock()

		if p.idle.Len() >= p.limit {
			return xerrors.WithStackTrace(errPoolOverflow)
		}

		if !p.internalPoolNotify(el) {
			p.idle.PushBack(el)
			// c.internalPoolPushIdle(s, c.clock.Now())
		}

		return nil
	}
}

// checked
// c.mu must be held.
func (p *pool[T]) internalPoolNotify(s *T) (notified bool) {
	for el := p.waitQ.Front(); el != nil; el = p.waitQ.Front() {
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
		ch := p.waitQ.Remove(el).(*chan *T)
		select {
		case *ch <- s:
			// Case (1).
			return true

		case <-p.done:
			// Case (2) or (3).
			close(*ch)

		default:
			// Case (2) or (3).
			close(*ch)
		}
	}
	return false
}

// checked
func (p *pool[T]) isClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}
