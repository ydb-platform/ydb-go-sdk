package pool

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type (
	itemInfo struct {
		idle    *list.Element
		touched time.Time
	}
	Pool[T any] struct {
		clock clockwork.Clock

		createItem func(ctx context.Context, onClose func(item *T)) (*T, error)
		deleteItem func(ctx context.Context, item *T) error
		checkErr   func(err error) bool

		mu                xsync.Mutex
		index             map[*T]itemInfo
		createInProgress  int
		limit             int        // Upper bound for Pool size.
		idle              *list.List // list<*T>
		waitQ             *list.List // list<*chan *T>
		waitChPool        sync.Pool
		testHookGetWaitCh func() // nil except some tests.
		wg                sync.WaitGroup
		done              chan struct{}
	}
	option[T any] func(p *Pool[T])
)

func New[T any](
	limit int,
	createItem func(ctx context.Context, onClose func(item *T)) (*T, error),
	deleteItem func(ctx context.Context, item *T) error,
	checkErr func(err error) bool,
	opts ...option[T],
) *Pool[T] {
	p := &Pool[T]{
		clock:      clockwork.NewRealClock(),
		createItem: createItem,
		deleteItem: deleteItem,
		checkErr:   checkErr,
		index:      make(map[*T]itemInfo),
		idle:       list.New(),
		waitQ:      list.New(),
		limit:      limit,
		waitChPool: sync.Pool{
			New: func() interface{} {
				ch := make(chan *T)

				return &ch
			},
		},
		done: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *Pool[T]) try(ctx context.Context, f func(ctx context.Context, item *T) error) error {
	item, err := p.get(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		select {
		case <-p.done:
			_ = p.deleteItem(ctx, item)
		default:
			p.mu.Lock()
			defer p.mu.Unlock()

			if p.idle.Len() >= p.limit {
				_ = p.deleteItem(ctx, item)
			}

			if !p.notify(item) {
				p.pushIdle(item, p.clock.Now())
			}
		}
	}()

	if err = f(ctx, item); err != nil {
		if p.checkErr(err) {
			_ = p.deleteItem(ctx, item)
		}

		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[T]) With(ctx context.Context, f func(ctx context.Context, item *T) error) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		err := p.try(ctx, f)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	})
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[T]) newItem(ctx context.Context) (item *T, err error) {
	select {
	case <-p.done:
		return nil, xerrors.WithStackTrace(errClosedPool)
	default:
		// pre-check the Client size
		var enoughSpace bool
		p.mu.WithLock(func() {
			enoughSpace = p.createInProgress+len(p.index) < p.limit
			if enoughSpace {
				p.createInProgress++
			}
		})

		if !enoughSpace {
			return nil, xerrors.WithStackTrace(errPoolOverflow)
		}

		defer func() {
			p.mu.WithLock(func() {
				p.createInProgress--
			})
		}()

		item, err = p.createItem(ctx, p.removeItem)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return item, nil
	}
}

func (p *Pool[T]) removeItem(item *T) {
	p.mu.WithLock(func() {
		info, has := p.index[item]
		if !has {
			return
		}

		delete(p.index, item)

		select {
		case <-p.done:
		default:
			p.notify(nil)
		}

		if info.idle != nil {
			p.idle.Remove(info.idle)
		}
	})
}

func (p *Pool[T]) get(ctx context.Context) (item *T, err error) {
	for {
		select {
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case <-p.done:
			return nil, xerrors.WithStackTrace(errClosedPool)
		default:
			// First, we try to get item from idle
			p.mu.WithLock(func() {
				item = p.removeFirstIdle()
			})
			if item != nil {
				return item, nil
			}

			// Second, we try to create item.
			item, _ = p.newItem(ctx)
			if item != nil {
				return item, nil
			}

			// Third, we try to wait for a touched item - Pool is full.
			//
			// This should be done only if number of currently waiting goroutines
			// are less than maximum amount of touched item. That is, we want to
			// be fair here and not to lock more goroutines than we could ship
			// item to.
			item, _ = p.waitFromCh(ctx)
			if item != nil {
				return item, nil
			}
		}
	}
}

func (p *Pool[T]) waitFromCh(ctx context.Context) (item *T, err error) {
	var (
		ch      *chan *T
		element *list.Element // Element in the wait queue.
		ok      bool
	)

	p.mu.WithLock(func() {
		ch = p.getWaitCh()
		element = p.waitQ.PushBack(ch)
	})

	select {
	case <-ctx.Done():
		p.mu.WithLock(func() {
			p.waitQ.Remove(element)
		})

		return nil, xerrors.WithStackTrace(ctx.Err())

	case <-p.done:
		p.mu.WithLock(func() {
			p.waitQ.Remove(element)
		})

		return nil, xerrors.WithStackTrace(errClosedPool)

	case item, ok = <-*ch:
		// Note that race may occur and some goroutine may try to write
		// item into channel after it was enqueued but before it being
		// read here. In that case we will receive nil here and will retry.
		//
		// The same way will work when some item become deleted - the
		// nil value will be sent into the channel.
		if ok {
			// Put only filled and not closed channel back to the Pool.
			// That is, we need to avoid races on filling reused channel
			// for the next waiter – item could be lost for a long time.
			p.putWaitCh(ch)
		}

		return item, nil
	}
}

// Close deletes all stored items inside Pool.
// It also stops all underlying timers and goroutines.
// It returns first error occurred during stale items' deletion.
// Note that even on error it calls Close() on each item.
func (p *Pool[T]) Close(ctx context.Context) (err error) {
	p.mu.WithLock(func() {
		select {
		case <-p.done:
			return

		default:
			close(p.done)

			p.limit = 0

			for element := p.waitQ.Front(); element != nil; element = element.Next() {
				ch := element.Value.(*chan *T)
				close(*ch)
			}

			for element := p.idle.Front(); element != nil; element = element.Next() {
				item := element.Value.(*T)
				p.wg.Add(1)
				go func() {
					defer p.wg.Done()
					_ = p.deleteItem(ctx, item)
				}()
			}
		}
	})

	p.wg.Wait()

	return nil
}

// getWaitCh returns pointer to a channel of items.
//
// Note that returning a pointer reduces allocations on sync.Pool usage –
// sync.Pool.Get() returns empty interface, which leads to allocation for
// non-pointer values.
func (p *Pool[T]) getWaitCh() *chan *T { //nolint:gocritic
	if p.testHookGetWaitCh != nil {
		p.testHookGetWaitCh()
	}
	ch := p.waitChPool.Get()
	s, ok := ch.(*chan *T)
	if !ok {
		panic(fmt.Sprintf("%T is not a chan of items", ch))
	}

	return s
}

// putWaitCh receives pointer to a channel and makes it available for further
// use.
// Note that ch MUST NOT be owned by any goroutine at the call moment and ch
// MUST NOT contain any value.
func (p *Pool[T]) putWaitCh(ch *chan *T) { //nolint:gocritic
	p.waitChPool.Put(ch)
}

// c.mu must be held.
func (p *Pool[T]) peekFirstIdle() (item *T, touched time.Time) {
	element := p.idle.Front()
	if element == nil {
		return
	}
	item = element.Value.(*T)
	info, has := p.index[item]
	if !has || element != info.idle {
		panic("inconsistent item in pool index")
	}

	return item, info.touched
}

// removes first item from idle and resets the keepAliveCount
// to prevent item from dying in the internalPoolGC after it was returned
// to be used only in outgoing functions that make item busy.
// c.mu must be held.
func (p *Pool[T]) removeFirstIdle() *T {
	item, _ := p.peekFirstIdle()
	if item != nil {
		p.index[item] = p.removeIdle(item)
	}

	return item
}

// c.mu must be held.
func (p *Pool[T]) notify(item *T) (notified bool) {
	for element := p.waitQ.Front(); element != nil; element = p.waitQ.Front() {
		// Some goroutine is waiting for a item.
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
		ch := p.waitQ.Remove(element).(*chan *T)
		select {
		case *ch <- item:
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

// c.mu must be held.
func (p *Pool[T]) removeIdle(item *T) itemInfo {
	info := p.index[item]
	p.idle.Remove(info.idle)
	info.idle = nil
	p.index[item] = info

	return info
}

// c.mu must be held.
func (p *Pool[T]) pushIdle(item *T, now time.Time) {
	p.handlePushIdle(item, now, p.idle.PushBack(item))
}

// c.mu must be held.
func (p *Pool[T]) handlePushIdle(item *T, now time.Time, element *list.Element) {
	info := p.index[item]
	info.touched = now
	info.idle = element
	p.index[item] = info
}
