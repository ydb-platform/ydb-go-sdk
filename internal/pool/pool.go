package pool

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
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
	Stats struct {
		locked   *xsync.Locked[stats.Stats]
		onChange func(stats.Stats)
	}
	Pool[PT Item[T], T any] struct {
		trace *Trace
		limit int

		create func(ctx context.Context) (PT, error)

		mu    xsync.Mutex
		idle  []PT
		index map[PT]struct{}

		done atomic.Bool

		stats *Stats
	}
	option[PT Item[T], T any] func(p *Pool[PT, T])
)

func (stats *Stats) Change(f func(s stats.Stats) stats.Stats) {
	stats.onChange(stats.locked.Change(f))
}

func (stats *Stats) Get() stats.Stats {
	return stats.locked.Get()
}

func WithCreateFunc[PT Item[T], T any](f func(ctx context.Context) (PT, error)) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.create = f
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
		trace: defaultTrace,
		limit: DefaultLimit,
		create: func(ctx context.Context) (PT, error) {
			var item T

			return &item, nil
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	onDone := p.trace.OnNew(&NewStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})

	defer func() {
		onDone(&NewDoneInfo{
			Limit: p.limit,
		})
	}()

	p.idle = make([]PT, 0, p.limit)
	p.index = make(map[PT]struct{}, p.limit)
	p.stats = &Stats{
		locked: xsync.NewLocked[stats.Stats](stats.Stats{
			Limit: p.limit,
		}),
		onChange: p.trace.OnChange,
	}

	return p
}

func (p *Pool[PT, T]) Stats() stats.Stats {
	return p.stats.Get()
}

func (p *Pool[PT, T]) get(ctx context.Context) (_ PT, finalErr error) {
	onDone := p.trace.OnGet(&GetStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&GetDoneInfo{
			Error: finalErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if p.done.Load() {
		return nil, xerrors.WithStackTrace(errClosedPool)
	}

	var item PT
	p.mu.WithLock(func() {
		if len(p.idle) > 0 {
			item, p.idle = p.idle[0], p.idle[1:]
			p.stats.Change(func(v stats.Stats) stats.Stats {
				v.Idle--

				return v
			})
		}
	})

	if item != nil {
		if item.IsAlive() {
			return item, nil
		}
		_ = item.Close(ctx)
		p.mu.WithLock(func() {
			delete(p.index, item)
		})
		p.stats.Change(func(v stats.Stats) stats.Stats {
			v.Index--

			return v
		})
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.index) == p.limit {
		return nil, xerrors.WithStackTrace(errPoolOverflow)
	}

	item, err := p.create(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	p.index[item] = struct{}{}
	p.stats.Change(func(v stats.Stats) stats.Stats {
		v.Index++

		return v
	})

	return item, nil
}

func (p *Pool[PT, T]) put(ctx context.Context, item PT) (finalErr error) {
	onDone := p.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&PutDoneInfo{
			Error: finalErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return xerrors.WithStackTrace(err)
	}

	if p.done.Load() {
		return xerrors.WithStackTrace(errClosedPool)
	}

	if !item.IsAlive() {
		_ = item.Close(ctx)

		p.mu.WithLock(func() {
			delete(p.index, item)
		})
		p.stats.Change(func(v stats.Stats) stats.Stats {
			v.Index--

			return v
		})

		return xerrors.WithStackTrace(errItemIsNotAlive)
	}

	p.mu.WithLock(func() {
		p.idle = append(p.idle, item)
	})
	p.stats.Change(func(v stats.Stats) stats.Stats {
		v.Idle++

		return v
	})

	return nil
}

func (p *Pool[PT, T]) try(ctx context.Context, f func(ctx context.Context, item PT) error) (finalErr error) {
	onDone := p.trace.OnTry(&TryStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&TryDoneInfo{
			Error: finalErr,
		})
	}()

	item, err := p.get(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		_ = p.put(ctx, item)
	}()

	p.stats.Change(func(v stats.Stats) stats.Stats {
		v.InUse++

		return v
	})
	defer func() {
		p.stats.Change(func(v stats.Stats) stats.Stats {
			v.InUse--

			return v
		})
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
			Call:    stack.FunctionID(""),
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
		OnRetry: func(info trace.RetryLoopStartInfo) func(trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
			return func(info trace.RetryLoopIntermediateInfo) func(trace.RetryLoopDoneInfo) {
				return func(info trace.RetryLoopDoneInfo) {
					attempts = info.Attempts
				}
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
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&CloseDoneInfo{
			Error: finalErr,
		})
	}()

	p.done.Store(true)

	p.mu.Lock()
	defer p.mu.Unlock()

	errs := make([]error, 0, len(p.index))

	for item := range p.index {
		if err := item.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return xerrors.Join(errs...)
	}
}
