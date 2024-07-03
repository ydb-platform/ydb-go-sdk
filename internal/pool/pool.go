package pool

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type (
	Item[T any] interface {
		*T
		IsAlive() bool
		Close(ctx context.Context) error
	}
	safeStats struct {
		mu       xsync.RWMutex
		v        stats.Stats
		onChange func(stats.Stats)
	}
	statsItemAddr struct {
		v        *int
		onChange func(func())
	}
	Pool[PT Item[T], T any] struct {
		trace *Trace
		limit int

		createItem    func(ctx context.Context) (PT, error)
		createTimeout time.Duration
		closeTimeout  time.Duration

		// queue is a buffered channel that holds ready-to-use items.
		// Newly created items are sent to this channel by spawner goroutine.
		// getItem reads from this channel to get items for usage.
		// putItems sends item to this channel when it's no longer needed.
		// Len of the buffered channel should be equal to configured pool size
		// (MUST NOT be less).
		// If item is in this queue, then it's considered idle (not in use).
		queue chan PT

		// itemTokens similarly to 'queue' is a buffered channel, and it holds 'tokens'.
		// Presence of token in this channel indicates that there's requests to create item.
		// Every token will eventually result in creation of new item (spawnItems makes sure of that).
		//
		// itemTokens must have same size as queue.
		// Sum of every existing token plus sum of every existing item in any time MUST be equal
		// to pool size. New token MUST be added by getItem/putItem if they discovered item in use to be
		// no good and discarded it.
		itemTokens chan struct{}

		done chan struct{}

		stats *safeStats
	}
	option[PT Item[T], T any] func(p *Pool[PT, T])
)

func (field statsItemAddr) Inc() {
	field.onChange(func() {
		*field.v++
	})
}

func (field statsItemAddr) Dec() {
	field.onChange(func() {
		*field.v--
	})
}

func (s *safeStats) Get() stats.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.v
}

func (s *safeStats) Index() statsItemAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return statsItemAddr{
		v: &s.v.Index,
		onChange: func(f func()) {
			s.mu.WithLock(f)
			if s.onChange != nil {
				s.onChange(s.Get())
			}
		},
	}
}

func (s *safeStats) Idle() statsItemAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return statsItemAddr{
		v: &s.v.Idle,
		onChange: func(f func()) {
			s.mu.WithLock(f)
			if s.onChange != nil {
				s.onChange(s.Get())
			}
		},
	}
}

func (s *safeStats) InUse() statsItemAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return statsItemAddr{
		v: &s.v.InUse,
		onChange: func(f func()) {
			s.mu.WithLock(f)
			if s.onChange != nil {
				s.onChange(s.Get())
			}
		},
	}
}

func WithCreateFunc[PT Item[T], T any](f func(ctx context.Context) (PT, error)) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.createItem = f
	}
}

func WithCreateItemTimeout[PT Item[T], T any](t time.Duration) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.createTimeout = t
	}
}

func WithCloseItemTimeout[PT Item[T], T any](t time.Duration) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.closeTimeout = t
	}
}

func WithLimit[PT Item[T], T any](size int) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.limit = size
	}
}

func WithTrace[PT Item[T], T any](t *Trace) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.trace = t
	}
}

func New[PT Item[T], T any](
	ctx context.Context,
	opts ...option[PT, T],
) *Pool[PT, T] {
	p := &Pool[PT, T]{
		trace:      defaultTrace,
		limit:      DefaultLimit,
		createItem: defaultCreateItem[T, PT],
		done:       make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	p.queue = make(chan PT, p.limit)
	p.itemTokens = make(chan struct{}, p.limit)
	go func() {
		// fill tokens
		for i := 0; i < p.limit; i++ {
			p.itemTokens <- struct{}{}
		}
	}()

	onDone := p.trace.OnNew(&NewStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.New"),
	})

	defer func() {
		onDone(&NewDoneInfo{
			Limit: p.limit,
		})
	}()

	p.createItem = createItemWithTimeoutHandling(p.createItem, p)

	p.stats = &safeStats{
		v:        stats.Stats{Limit: p.limit},
		onChange: p.trace.OnChange,
	}

	go p.spawnItems(xcontext.ValueOnly(ctx))

	return p
}

// spawnItems creates one item per each available itemToken and sends new item to internal item queue.
// It ensures that pool would always have amount of connections equal to configured limit.
// If item creation ended with error it will be retried infinity with configured interval until success.
func (p *Pool[PT, T]) spawnItems(ctx context.Context) {
	for {
		select {
		case <-p.done:
			return
		case <-p.itemTokens:
			// got token, must create item
			for {
				err := p.trySpawn(ctx)
				if err == nil {
					break
				}
				// spawn was unsuccessful, need to try again.
				// token must always result in new item and not be lost.
			}
		}
	}
}

func (p *Pool[PT, T]) trySpawn(ctx context.Context) error {
	item, err := p.createItem(ctx)
	if err != nil {
		return err
	}
	// item was created successfully, put it in queue
	select {
	case <-p.done:
		return nil
	case p.queue <- item:
		p.stats.Idle().Inc()
	}

	return nil
}

// defaultCreateItem returns a new item
func defaultCreateItem[T any, PT Item[T]](ctx context.Context) (PT, error) {
	var item T

	return &item, nil
}

// createItemWithTimeoutHandling wraps the createItem function with timeout handling
func createItemWithTimeoutHandling[PT Item[T], T any](
	createItem func(ctx context.Context) (PT, error),
	p *Pool[PT, T],
) func(ctx context.Context) (PT, error) {
	return func(ctx context.Context) (PT, error) {
		var (
			ch        = make(chan PT)
			createErr error
		)
		go func() {
			defer close(ch)
			createErr = createItemWithContext(ctx, p, createItem, ch)
		}()

		select {
		case <-p.done:
			return nil, xerrors.WithStackTrace(errClosedPool)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case item, has := <-ch:
			if !has {
				if ctxErr := ctx.Err(); ctxErr == nil && xerrors.IsContextError(createErr) {
					return nil, xerrors.WithStackTrace(xerrors.Retryable(createErr))
				}

				return nil, xerrors.WithStackTrace(createErr)
			}

			return item, nil
		}
	}
}

// createItemWithContext handles the creation of an item with context handling
func createItemWithContext[PT Item[T], T any](
	ctx context.Context,
	p *Pool[PT, T],
	createItem func(ctx context.Context) (PT, error),
	ch chan PT,
) error {
	var (
		createCtx    = xcontext.ValueOnly(ctx)
		cancelCreate context.CancelFunc
	)

	if d := p.createTimeout; d > 0 {
		createCtx, cancelCreate = xcontext.WithTimeout(createCtx, d)
	} else {
		createCtx, cancelCreate = xcontext.WithCancel(createCtx)
	}
	defer cancelCreate()

	newItem, err := createItem(createCtx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case ch <- newItem:
		return nil
	}
}

func (p *Pool[PT, T]) Stats() stats.Stats {
	return p.stats.Get()
}

// getItem retrieves item from the queue.
// If retrieved item happens to be not alive, then it's destroyed
// and tokens queue is filled to +1 so new item can be created by spawner goroutine.
// After, the process will be repeated until alive item is retrieved.
func (p *Pool[PT, T]) getItem(ctx context.Context) (_ PT, finalErr error) {
	onDone := p.trace.OnGet(&GetStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.(*Pool).getItem"),
	})
	defer func() {
		onDone(&GetDoneInfo{
			Error: finalErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	// get item and ensure it's alive.
	// Infinite loop here guarantees that we either return alive item
	// or block infinitely until we have one.
	// It is assumed that calling code should use context if it wishes to time out the call.
	for {
		select {
		case <-p.done:
			return nil, xerrors.WithStackTrace(errClosedPool)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case item := <-p.queue: // get or wait for item
			p.stats.Idle().Dec()
			if item != nil {
				if item.IsAlive() {
					// item is alive, return it

					return item, nil
				}
				// item is not alive
				_ = p.closeItem(ctx, item) // clean up dead item
			}
			p.itemTokens <- struct{}{} // signal spawn goroutine to create a new item

			// and try again
		}
	}
}

func (p *Pool[PT, T]) putItem(ctx context.Context, item PT) (finalErr error) {
	onDone := p.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.(*Pool).putItem"),
	})
	defer func() {
		onDone(&PutDoneInfo{
			Error: finalErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	default:
		if item.IsAlive() {
			// put back in the queue
			select {
			case <-p.done:
				return xerrors.WithStackTrace(errClosedPool)
			case <-ctx.Done():
				return xerrors.WithStackTrace(ctx.Err())
			case p.queue <- item:
				p.stats.Idle().Inc()
			}
		} else {
			// item is not alive
			// add token and close
			p.itemTokens <- struct{}{}
			_ = p.closeItem(ctx, item)
		}
	}

	return nil
}

func (p *Pool[PT, T]) closeItem(ctx context.Context, item PT) error {
	ctx = xcontext.ValueOnly(ctx)

	var cancel context.CancelFunc
	if d := p.closeTimeout; d > 0 {
		ctx, cancel = xcontext.WithTimeout(ctx, d)
	} else {
		ctx, cancel = xcontext.WithCancel(ctx)
	}
	defer cancel()

	return item.Close(ctx)
}

func (p *Pool[PT, T]) try(ctx context.Context, f func(ctx context.Context, item PT) error) (finalErr error) {
	onDone := p.trace.OnTry(&TryStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.(*Pool).try"),
	})
	defer func() {
		onDone(&TryDoneInfo{
			Error: finalErr,
		})
	}()

	item, err := p.getItem(ctx)
	if err != nil {
		if xerrors.IsYdb(err) {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		return xerrors.WithStackTrace(err)
	}
	p.stats.InUse().Inc()

	defer func() {
		_ = p.putItem(ctx, item)
		p.stats.InUse().Dec()
	}()

	err = f(ctx, item)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[PT, T]) With(
	ctx context.Context,
	f func(ctx context.Context, item PT) error,
	opts ...retry.Option,
) (finalErr error) {
	var (
		onDone = p.trace.OnWith(&WithStartInfo{
			Context: &ctx,
			Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.(*Pool).With"),
		})
		attempts int
	)
	defer func() {
		onDone(&WithDoneInfo{
			Error:    finalErr,
			Attempts: attempts,
		})
	}()

	err := retry.Retry(ctx, func(ctx context.Context) error {
		err := p.try(ctx, f)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}, append(opts, retry.WithTrace(&trace.Retry{
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopDoneInfo) {
				attempts = info.Attempts
			}
		},
	}))...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (p *Pool[PT, T]) Close(ctx context.Context) (finalErr error) {
	onDone := p.trace.OnClose(&CloseStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/3/internal/pool.(*Pool).Close"),
	})
	defer func() {
		onDone(&CloseDoneInfo{
			Error: finalErr,
		})
	}()

	// Only closing done channel.
	// Due to multiple senders queue is not closed here,
	// we're just making sure to drain it fully to close any existing item.
	close(p.done)
	var g errgroup.Group
shutdownLoop:
	for {
		select {
		case item := <-p.queue:
			g.Go(func() error {
				return item.Close(ctx)
			})
		default:
			break shutdownLoop
		}
	}
	if err := g.Wait(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}
