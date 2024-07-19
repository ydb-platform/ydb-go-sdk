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
	safeStats struct {
		mu       xsync.RWMutex
		v        Stats
		onChange func(Stats)
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

		mu    xsync.Mutex
		idle  []PT
		index map[PT]struct{}
		done  chan struct{}

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

func (s *safeStats) Get() Stats {
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

	p.idle = make([]PT, 0, p.limit)
	p.index = make(map[PT]struct{}, p.limit)
	p.stats = &safeStats{
		v:        Stats{Limit: p.limit},
		onChange: p.trace.OnChange,
	}

	return p
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

	needCloseItem := true
	defer func() {
		if needCloseItem {
			_ = p.closeItem(ctx, newItem)
		}
	}()

	select {
	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)
	case <-ctx.Done():
		p.mu.Lock()
		defer p.mu.Unlock()

		if len(p.index) < p.limit {
			p.idle = append(p.idle, newItem)
			p.index[newItem] = struct{}{}
			p.stats.Index().Inc()
			needCloseItem = false
		}

		return xerrors.WithStackTrace(ctx.Err())
	case ch <- newItem:
		needCloseItem = false

		return nil
	}
}

func (p *Pool[PT, T]) Stats() Stats {
	return p.stats.Get()
}

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

	select {
	case <-p.done:
		return nil, xerrors.WithStackTrace(errClosedPool)
	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())
	default:
		var item PT
		p.mu.WithLock(func() {
			if len(p.idle) > 0 {
				item, p.idle = p.idle[0], p.idle[1:]
				p.stats.Idle().Dec()
			}
		})

		if item != nil {
			if item.IsAlive() {
				return item, nil
			}
			_ = p.closeItem(ctx, item)
			p.mu.WithLock(func() {
				delete(p.index, item)
			})
			p.stats.Index().Dec()
		}

		item, err := p.createItem(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		addedToIndex := false
		p.mu.WithLock(func() {
			if len(p.index) < p.limit {
				p.index[item] = struct{}{}
				addedToIndex = true
			}
		})
		if addedToIndex {
			p.stats.Index().Inc()
		}

		return item, nil
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
	default:
		if !item.IsAlive() {
			_ = p.closeItem(ctx, item)

			p.mu.WithLock(func() {
				delete(p.index, item)
			})
			p.stats.Index().Dec()

			return xerrors.WithStackTrace(errItemIsNotAlive)
		}

		p.mu.WithLock(func() {
			p.idle = append(p.idle, item)
		})
		p.stats.Idle().Inc()

		return nil
	}
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

	defer func() {
		_ = p.putItem(ctx, item)
	}()

	p.stats.InUse().Inc()
	defer p.stats.InUse().Dec()

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
