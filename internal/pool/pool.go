package pool

import (
	"context"
	"sync"
	"time"

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
	Config[PT Item[T], T any] struct {
		trace         *Trace
		limit         int
		createItem    func(ctx context.Context) (PT, error)
		createTimeout time.Duration
		closeTimeout  time.Duration
	}
	Pool[PT Item[T], T any] struct {
		config Config[PT, T]

		createItem func(ctx context.Context) (PT, error)
		closeItem  func(ctx context.Context, item PT)
		sema       chan struct{}

		mu   xsync.RWMutex
		idle []PT

		done chan struct{}
	}
	option[PT Item[T], T any] func(p *Config[PT, T])
)

func WithCreateFunc[PT Item[T], T any](f func(ctx context.Context) (PT, error)) option[PT, T] {
	return func(c *Config[PT, T]) {
		c.createItem = f
	}
}

func WithCreateItemTimeout[PT Item[T], T any](t time.Duration) option[PT, T] {
	return func(c *Config[PT, T]) {
		c.createTimeout = t
	}
}

func WithCloseItemTimeout[PT Item[T], T any](t time.Duration) option[PT, T] {
	return func(c *Config[PT, T]) {
		c.closeTimeout = t
	}
}

func WithLimit[PT Item[T], T any](size int) option[PT, T] {
	return func(c *Config[PT, T]) {
		c.limit = size
	}
}

func WithTrace[PT Item[T], T any](t *Trace) option[PT, T] {
	return func(c *Config[PT, T]) {
		c.trace = t
	}
}

func New[PT Item[T], T any](
	ctx context.Context,
	opts ...option[PT, T],
) *Pool[PT, T] {
	p := &Pool[PT, T]{
		config: Config[PT, T]{
			trace:      defaultTrace,
			limit:      DefaultLimit,
			createItem: defaultCreateItem[T, PT],
		},
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&p.config)
		}
	}

	onDone := p.config.trace.OnNew(&NewStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.New"),
	})

	defer func() {
		onDone(&NewDoneInfo{
			Limit: p.config.limit,
		})
	}()

	p.createItem = makeCreateItemFunc(p.config, p.done, func(item PT) error {
		return xsync.WithLock(&p.mu, func() error {
			if len(p.idle) >= p.config.limit {
				return xerrors.WithStackTrace(errPoolIsOverflow)
			}

			p.appendItemToIdle(item)

			return nil
		})
	})
	p.closeItem = makeAsyncCloseItemFunc[PT, T](
		p.config.closeTimeout, p.done,
	)
	p.sema = make(chan struct{}, p.config.limit)
	p.idle = make([]PT, 0, p.config.limit)

	return p
}

// defaultCreateItem returns a new item
func defaultCreateItem[T any, PT Item[T]](context.Context) (PT, error) {
	var item T

	return &item, nil
}

// makeCreateItemFunc wraps the createItem function with timeout handling
func makeCreateItemFunc[PT Item[T], T any]( //nolint:funlen
	config Config[PT, T],
	done <-chan struct{},
	appendToIdle func(item PT) error,
) func(ctx context.Context) (PT, error) {
	return func(ctx context.Context) (PT, error) {
		var ch = make(chan struct {
			item PT
			err  error
		})

		go func() {
			defer close(ch)

			createCtx, cancelCreate := xcontext.WithDone(xcontext.ValueOnly(ctx), done)
			defer cancelCreate()

			if d := config.createTimeout; d > 0 {
				createCtx, cancelCreate = xcontext.WithTimeout(createCtx, d)
				defer cancelCreate()
			}

			newItem, err := config.createItem(createCtx)

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

				if appendToIdleErr := appendToIdle(newItem); appendToIdleErr != nil {
					_ = newItem.Close(ctx)
				}
			}
		}()

		select {
		case <-done:
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

func (p *Pool[PT, T]) onChangeStats() {
	p.mu.RLock()
	info := ChangeInfo{
		Limit: p.config.limit,
		Idle:  len(p.idle),
	}
	p.mu.RUnlock()
	p.config.trace.OnChange(info)
}

func (p *Pool[PT, T]) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return Stats{
		Limit: p.config.limit,
		Idle:  len(p.idle),
	}
}

func (p *Pool[PT, T]) getItemFromIdle() (item PT) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.idle) == 0 {
		return nil
	}

	item, p.idle = p.idle[0], p.idle[1:]
	go p.onChangeStats()

	return item
}

func (p *Pool[PT, T]) getItem(ctx context.Context) (_ PT, finalErr error) {
	onDone := p.config.trace.OnGet(&GetStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).getItem"),
	})
	defer func() {
		onDone(&GetDoneInfo{
			Error: finalErr,
		})
	}()

	item := p.getItemFromIdle()

	if item != nil {
		if item.IsAlive() {
			return item, nil
		}

		p.closeItem(ctx, item)

		return nil, xerrors.WithStackTrace(xerrors.Retryable(errItemIsNotAlive))
	}

	newItem, err := p.createItem(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(xerrors.Retryable(err))
	}

	return newItem, nil
}

// p.mu must be locked
func (p *Pool[PT, T]) appendItemToIdle(item PT) {
	p.idle = append(p.idle, item)
	go p.onChangeStats()
}

func (p *Pool[PT, T]) putItem(ctx context.Context, item PT) (finalErr error) {
	onDone := p.config.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).putItem"),
	})
	defer func() {
		onDone(&PutDoneInfo{
			Error: finalErr,
		})
	}()

	if !item.IsAlive() {
		p.closeItem(ctx, item)

		return xerrors.WithStackTrace(errItemIsNotAlive)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.done:
		_ = item.Close(ctx)

		return xerrors.WithStackTrace(errClosedPool)
	default:
		if len(p.idle) >= p.config.limit {
			p.closeItem(ctx, item)

			return xerrors.WithStackTrace(errPoolIsOverflow)
		}

		p.appendItemToIdle(item)

		return nil
	}
}

func makeAsyncCloseItemFunc[PT Item[T], T any](
	closeTimeout time.Duration,
	done <-chan struct{},
) func(ctx context.Context, item PT) {
	return func(ctx context.Context, item PT) {
		closeItemCtx, closeItemCancel := xcontext.WithDone(xcontext.ValueOnly(ctx), done)
		defer closeItemCancel()

		if d := closeTimeout; d > 0 {
			closeItemCtx, closeItemCancel = xcontext.WithTimeout(ctx, d)
			defer closeItemCancel()
		}

		go func() {
			_ = item.Close(closeItemCtx)
		}()
	}
}

func (p *Pool[PT, T]) try(ctx context.Context, f func(ctx context.Context, item PT) error) (finalErr error) {
	onDone := p.config.trace.OnTry(&TryStartInfo{
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
		go p.onChangeStats()
		defer func() {
			<-p.sema
			go p.onChangeStats()
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
		onDone = p.config.trace.OnWith(&WithStartInfo{
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
	onDone := p.config.trace.OnClose(&CloseStartInfo{
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

	errs := xsync.Set[error]{}

	var wg sync.WaitGroup
	wg.Add(len(p.idle))

	for _, item := range p.idle {
		go func(item PT) {
			defer wg.Done()

			if err := item.Close(ctx); err != nil {
				errs.Add(err)
			}
		}(item)
	}
	wg.Wait()

	p.idle = nil

	if errs.Size() > 0 {
		return xerrors.WithStackTrace(xerrors.Join(errs.Values()...))
	}

	return nil
}
