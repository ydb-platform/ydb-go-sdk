package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
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
	lazyItem[PT Item[T], T any] struct {
		mu         xsync.Mutex
		item       PT
		createItem func(ctx context.Context) (PT, error)
	}
	Pool[PT Item[T], T any] struct {
		trace *Trace
		limit int

		createItem    func(ctx context.Context) (PT, error)
		createTimeout time.Duration
		closeTimeout  time.Duration

		items chan *lazyItem[PT, T]
		done  chan struct{}
	}
	option[PT Item[T], T any] func(p *Pool[PT, T])
)

func (p *Pool[PT, T]) Stats() Stats {
	return Stats{
		Limit: p.limit,
		Idle:  len(p.items),
		InUse: 0,
	}
}

func (item *lazyItem[PT, T]) get(ctx context.Context) (_ PT, err error) {
	item.mu.Lock()
	defer item.mu.Unlock()

	if item.item != nil {
		if item.item.IsAlive() {
			return item.item, nil
		}

		_ = item.item.Close(ctx)
	}

	item.item, err = item.createItem(ctx)

	return item.item, err
}

func (item *lazyItem[PT, T]) close(ctx context.Context) error {
	item.mu.Lock()
	defer item.mu.Unlock()

	if item.item == nil {
		return nil
	}

	defer func() {
		item.item = nil
	}()

	return item.item.Close(ctx)
}

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

func New[PT Item[T], T any]( //nolint:funlen
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

	createItem := p.createItem

	p.createItem = func(ctx context.Context) (PT, error) {
		ctx, cancel := xcontext.WithDone(ctx, p.done)
		defer cancel()

		var (
			createCtx    = xcontext.ValueOnly(ctx)
			cancelCreate context.CancelFunc
		)

		if t := p.createTimeout; t > 0 {
			createCtx, cancelCreate = xcontext.WithTimeout(createCtx, t)
		} else {
			createCtx, cancelCreate = xcontext.WithCancel(createCtx)
		}
		defer cancelCreate()

		newItem, err := createItem(createCtx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return newItem, nil
	}

	p.items = make(chan *lazyItem[PT, T], p.limit)
	for i := 0; i < p.limit; i++ {
		p.items <- &lazyItem[PT, T]{
			createItem: p.createItem,
		}
	}

	return p
}

// defaultCreateItem returns a new item
func defaultCreateItem[T any, PT Item[T]](ctx context.Context) (PT, error) {
	var item T

	return &item, nil
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

	select {
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())

	case <-p.done:
		return xerrors.WithStackTrace(errClosedPool)

	case lease, ok := <-p.items:
		if !ok {
			return xerrors.WithStackTrace(errClosedPool)
		}

		defer func() {
			p.items <- lease
		}()

		item, err := lease.get(ctx)
		if err != nil {
			if ctx.Err() == nil {
				return xerrors.WithStackTrace(xerrors.Retryable(err))
			}

			return xerrors.WithStackTrace(err)
		}

		if !item.IsAlive() {
			return xerrors.WithStackTrace(xerrors.Retryable(errItemIsNotAlive))
		}

		err = f(ctx, item)
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		return nil
	}
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
	}, opts...)
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

	errs := make([]error, 0, p.limit)

	for i := 0; i < p.limit; i++ {
		select {
		case <-ctx.Done():
			return xerrors.WithStackTrace(fmt.Errorf("%d items not closed: %w", p.limit-i, ctx.Err()))
		case item := <-p.items:
			if err := item.close(ctx); err != nil {
				errs = append(errs, err)
			}
		}
	}

	close(p.items)

	if len(errs) > 0 {
		return xerrors.WithStackTrace(xerrors.Join(errs...))
	}

	return nil
}
