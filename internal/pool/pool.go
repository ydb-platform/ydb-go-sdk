package pool

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

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
	Pool[PT Item[T], T any] struct {
		trace *Trace
		limit int

		createItem    func(ctx context.Context) (PT, error)
		createTimeout time.Duration
		closeTimeout  time.Duration

		sema chan struct{}

		mu    xsync.RWMutex
		idle  []PT
		index map[PT]struct{}
		done  chan struct{}

		stats *Stats
	}
	option[PT Item[T], T any] func(p *Pool[PT, T])
)

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

	onDone := p.trace.OnNew(&NewStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.New"),
	})

	defer func() {
		onDone(&NewDoneInfo{
			Limit: p.limit,
		})
	}()

	p.createItem = asyncCreateItemWithTimeout(p.createItem, p)
	p.sema = make(chan struct{}, p.limit)
	p.idle = make([]PT, 0, p.limit)
	p.index = make(map[PT]struct{}, p.limit)
	p.stats = &Stats{Limit: p.limit}

	return p
}

// defaultCreateItem returns a new item
func defaultCreateItem[T any, PT Item[T]](ctx context.Context) (PT, error) {
	var item T

	return &item, nil
}

// asyncCreateItemWithTimeout wraps the createItem function with timeout handling
func asyncCreateItemWithTimeout[PT Item[T], T any](
	createItem func(ctx context.Context) (PT, error),
	p *Pool[PT, T],
) func(ctx context.Context) (PT, error) {
	return func(ctx context.Context) (PT, error) {
		var ch = make(chan struct {
			item PT
			err  error
		})

		go func() {
			defer close(ch)

			createCtx, cancelCreate := xcontext.WithDone(xcontext.ValueOnly(ctx), p.done)
			defer cancelCreate()

			if d := p.createTimeout; d > 0 {
				createCtx, cancelCreate = xcontext.WithTimeout(createCtx, d)
				defer cancelCreate()
			}

			p.mu.WithLock(func() {
				p.stats.CreateInProgress++
			})

			newItem, err := createItem(createCtx)
			p.mu.WithLock(func() {
				p.stats.CreateInProgress--
			})

			select {
			case ch <- struct {
				item PT
				err  error
			}{
				item: newItem,
				err:  xerrors.WithStackTrace(err),
			}:
			default:
				if newItem == nil {
					return
				}

				if !xsync.WithLock(&p.mu, func() bool {
					if len(p.idle) >= p.limit {
						return false // not appended
					}

					p.idle = append(p.idle, newItem)
					p.stats.Idle++

					p.index[newItem] = struct{}{}
					p.stats.Index++

					return true // // item appended
				}) {
					_ = newItem.Close(ctx)
				}
			}
		}()

		select {
		case <-p.done:
			return nil, xerrors.WithStackTrace(errClosedPool)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		case result, has := <-ch:
			if !has {
				return nil, xerrors.WithStackTrace(errNoProgress)
			}

			if result.err != nil {
				if xerrors.IsContextError(result.err) {
					return nil, xerrors.WithStackTrace(xerrors.Retryable(result.err))
				}

				return nil, xerrors.WithStackTrace(result.err)
			}

			return result.item, nil
		}
	}
}

func (p *Pool[PT, T]) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return *p.stats
}

func (p *Pool[PT, T]) getItem(ctx context.Context) (item PT, finalErr error) {
	onDone := p.trace.OnGet(&GetStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).getItem"),
	})
	defer func() {
		onDone(&GetDoneInfo{
			Error: finalErr,
		})
	}()

	p.mu.Lock()
	if len(p.idle) > 0 {
		item, p.idle = p.idle[0], p.idle[1:]
		p.stats.Idle--
	}
	p.mu.Unlock()

	if item != nil {
		if item.IsAlive() {
			return item, nil
		}

		_ = p.closeItem(ctx, item)

		return nil, xerrors.WithStackTrace(xerrors.Retryable(errItemIsNotAlive))
	}

	newItem, err := p.createItem(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(err))
	}

	p.mu.WithLock(func() {
		p.stats.Index++
		p.index[newItem] = struct{}{}
	})

	return newItem, nil
}

func (p *Pool[PT, T]) putItem(ctx context.Context, item PT) (finalErr error) {
	onDone := p.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).putItem"),
	})
	defer func() {
		onDone(&PutDoneInfo{
			Error: finalErr,
		})
	}()

	if !item.IsAlive() {
		_ = p.closeItem(ctx, item)

		return xerrors.WithStackTrace(errItemIsNotAlive)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.idle) >= p.limit {
		_ = p.closeItem(ctx, item)

		return xerrors.WithStackTrace(errPoolIsOverflow)
	}

	p.idle = append(p.idle, item)
	p.stats.Idle--

	return nil
}

func (p *Pool[PT, T]) closeItem(ctx context.Context, item PT) error {
	var cancel context.CancelFunc
	if d := p.closeTimeout; d > 0 {
		ctx, cancel = xcontext.WithTimeout(ctx, d)
	} else {
		ctx, cancel = xcontext.WithCancel(ctx)
	}
	defer cancel()

	defer func() {
		p.mu.WithLock(func() {
			delete(p.index, item)
			p.stats.Index--
		})
	}()

	return item.Close(ctx)
}

func (p *Pool[PT, T]) try(ctx context.Context, f func(ctx context.Context, item PT) error) (finalErr error) {
	onDone := p.trace.OnTry(&TryStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try"),
	})
	defer func() {
		onDone(&TryDoneInfo{
			Error: finalErr,
		})
	}()

	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case p.sema <- struct{}{}:
		defer func() {
			<-p.sema
		}()
	}

	item, err := p.getItem(ctx)
	if err != nil {
		if xerrors.IsYdb(err) {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		return xerrors.WithStackTrace(err)
	}

	defer func() {
		_ = p.putItem(ctx, item)
	}()

	p.mu.Lock()
	p.stats.InUse++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.stats.InUse--
		p.mu.Unlock()
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
			Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).With"),
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
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).Close"),
	})
	defer func() {
		onDone(&CloseDoneInfo{
			Error: finalErr,
		})
	}()

	close(p.done)

	p.mu.Lock()
	defer p.mu.Unlock()

	var g errgroup.Group
	for item := range p.index {
		item := item
		g.Go(func() error {
			return item.Close(ctx)
		})
	}
	if err := g.Wait(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}
