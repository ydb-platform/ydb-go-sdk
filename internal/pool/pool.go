package pool

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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
		trace          *Trace
		maxSize        int
		minSize        int
		producersCount int

		create func(ctx context.Context) (PT, error)

		idle  chan PT
		spawn chan PT
		stop  chan struct{}
	}
	option[PT Item[T], T any] func(p *Pool[PT, T])
)

func WithCreateFunc[PT Item[T], T any](f func(ctx context.Context) (PT, error)) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.create = f
	}
}

func WithMinSize[PT Item[T], T any](size int) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.minSize = size
	}
}

func WithMaxSize[PT Item[T], T any](size int) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.maxSize = size
	}
}

func WithProducersCount[PT Item[T], T any](count int) option[PT, T] {
	return func(p *Pool[PT, T]) {
		p.producersCount = count
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
) (p *Pool[PT, T], finalErr error) {
	p = &Pool[PT, T]{
		trace:          defaultTrace,
		maxSize:        DefaultMaxSize,
		minSize:        DefaultMinSize,
		producersCount: DefaultProducersCount,
		create: func(ctx context.Context) (PT, error) {
			var item T

			return &item, nil
		},
		stop: make(chan struct{}),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	onDone := p.trace.OnNew(&NewStartInfo{
		Context:        &ctx,
		Call:           stack.FunctionID(""),
		MinSize:        p.minSize,
		MaxSize:        p.maxSize,
		ProducersCount: p.producersCount,
	})

	defer func() {
		onDone(&NewDoneInfo{
			Error:          finalErr,
			MinSize:        p.minSize,
			MaxSize:        p.maxSize,
			ProducersCount: p.producersCount,
		})
	}()

	if p.minSize > p.maxSize {
		p.minSize = p.maxSize / 10
	}

	if p.producersCount > p.maxSize {
		p.producersCount = p.maxSize
	}

	if p.producersCount <= 0 {
		p.producersCount = 1
	}

	p.idle = make(chan PT, p.maxSize)

	p.produce(ctx)

	return p, nil
}

func (p *Pool[PT, T]) want(ctx context.Context) (err error) {
	onDone := p.trace.OnWant(&WantStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&WantDoneInfo{
			Error: err,
		})
	}()

	select {
	case <-ctx.Done():
		return xerrors.WithStackTrace(ctx.Err())
	case p.spawn <- nil:
		return nil
	}
}

func (p *Pool[PT, T]) put(ctx context.Context, item PT) (err error) {
	onDone := p.trace.OnPut(&PutStartInfo{
		Context: &ctx,
		Call:    stack.FunctionID(""),
	})
	defer func() {
		onDone(&PutDoneInfo{
			Error: err,
		})
	}()

	select {
	case <-ctx.Done():
		// context is done
		return xerrors.WithStackTrace(ctx.Err())
	case <-p.stop:
		// pool closed early
		return item.Close(ctx)
	case p.spawn <- item:
		// return item into pool
		return nil
	default:
		// not enough space in pool
		return item.Close(ctx)
	}
}

func (p *Pool[PT, T]) produce(ctx context.Context) {
	onDone := p.trace.OnProduce(&ProduceStartInfo{
		Context:     &ctx,
		Call:        stack.FunctionID(""),
		Concurrency: p.producersCount,
	})
	defer func() {
		onDone(&ProduceDoneInfo{})
	}()

	p.spawn = make(chan PT, p.maxSize)

	var wg, started sync.WaitGroup
	wg.Add(p.producersCount)
	started.Add(p.producersCount + 1)

	for range make([]struct{}, p.producersCount) {
		go func() {
			defer wg.Done()
			started.Done()

			for {
				select {
				case <-p.stop:
					return

				case msg := <-p.spawn:
					if msg != nil {
						p.idle <- msg
					} else {
						item, err := p.create(context.Background())
						if err == nil {
							p.idle <- item
						}
					}
				}
			}
		}()
	}

	for i := 0; i < p.maxSize && len(p.idle) < p.minSize; i++ {
		_ = p.want(ctx)
	}

	go func() {
		started.Done()
		defer func() {
			close(p.idle)
		}()
		wg.Wait()
	}()

	started.Wait()
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
		if !item.IsAlive() {
			_ = item.Close(ctx)
		} else {
			_ = p.put(ctx, item)
		}
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

	retryCtx, cancelRetry := xcontext.WithDone(ctx, p.stop)
	defer cancelRetry()

	err := retry.Retry(retryCtx, func(ctx context.Context) error {
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

	for {
		select {
		case <-p.stop:
			return nil, xerrors.WithStackTrace(errClosedPool)
		case item, has := <-p.idle:
			if !has {
				return nil, xerrors.WithStackTrace(errClosedPool)
			}
			if item.IsAlive() {
				return item, nil
			}
			_ = item.Close(ctx)
		case p.spawn <- nil:
			continue
		}
	}
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

	close(p.stop)

	errs := make([]error, 0, len(p.idle)+len(p.spawn))

	for item := range p.idle {
		if err := item.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	for len(p.spawn) > 0 {
		if msg := <-p.spawn; msg != nil {
			if err := msg.Close(ctx); err != nil {
				errs = append(errs, err)
			}
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
