package pool

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
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
		closer.Closer
		IsAlive() bool
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
	lazyItem[PT Item[T], T any] struct {
		mutex      xsync.RWMutex
		createItem func(ctx context.Context) (PT, error)
		item       PT
	}
	Pool[PT Item[T], T any] struct {
		trace *Trace
		limit int

		createItem    func(ctx context.Context) (PT, error)
		createTimeout time.Duration
		closeTimeout  time.Duration

		idle chan *lazyItem[PT, T]

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

	p.idle = make(chan *lazyItem[PT, T], p.limit)
	createItem := createItemWithTimeoutHandling(p.createItem, p)
	for range make([]struct{}, p.limit) {
		p.idle <- &lazyItem[PT, T]{
			createItem: createItem,
		}
	}
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
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case ch <- newItem:
		needCloseItem = false

		return nil
	}
}

func (p *Pool[PT, T]) Stats() Stats {
	return p.stats.Get()
}

func (p *Pool[PT, T]) getItem(ctx context.Context) (_ *lazyItem[PT, T], finalErr error) {
	onDone := p.trace.OnGet(&GetStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).getItem"),
	})
	defer func() {
		onDone(&GetDoneInfo{
			Error: finalErr,
		})
	}()

	select {
	case <-ctx.Done():
		return nil, xerrors.WithStackTrace(ctx.Err())

	case idle, has := <-p.idle:
		if !has {
			return nil, xerrors.WithStackTrace(errClosedPool)
		}

		idle.mutex.Lock()
		defer idle.mutex.Unlock()

		if idle.item != nil && idle.item.IsAlive() {
			return idle, nil
		}

		item, err := p.createItem(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(
				xerrors.Retryable(err, xerrors.WithName("internal/pool.(*Pool).getItem")),
			)
		}

		idle.item = item

		return idle, nil
	}
}

func (p *Pool[PT, T]) putItem(ctx context.Context, idle *lazyItem[PT, T]) (finalErr error) {
	onDone := p.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).putItem"),
	})
	defer func() {
		p.idle <- idle

		onDone(&PutDoneInfo{
			Error: finalErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	idle.mutex.Lock()
	defer idle.mutex.Unlock()

	if idle.item != nil && !idle.item.IsAlive() {
		_ = idle.item.Close(ctx)
		idle.item = nil

		return xerrors.WithStackTrace(errItemIsNotAlive)
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
		Call:    stack.FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/pool.(*Pool).try"),
	})
	defer func() {
		onDone(&TryDoneInfo{
			Error: finalErr,
		})
	}()

	idle, err := p.getItem(ctx)
	if err != nil {
		if xerrors.IsYdb(err) {
			return xerrors.WithStackTrace(xerrors.Retryable(err))
		}

		return xerrors.WithStackTrace(err)
	}

	defer func() {
		_ = p.putItem(ctx, idle)
	}()

	err = f(ctx, idle.item)
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

	for range make([]struct{}, p.limit) {
		item := <-p.idle
		item.mutex.WithLock(func() {
			if item.item != nil {
				_ = item.item.Close(ctx)
				item.item = nil
			}
		})
	}

	close(p.idle)

	return nil
}
