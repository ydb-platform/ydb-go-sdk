package background

import (
	"context"
	"runtime/pprof"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// A Worker must not be copied after first use
type Worker struct {
	ctx     context.Context
	workers sync.WaitGroup

	onceInit sync.Once

	m    xsync.Mutex
	stop xcontext.CancelErrFunc
}

func NewWorker(parent context.Context) *Worker {
	w := Worker{}
	w.ctx, w.stop = xcontext.WithErrCancel(parent)

	return &w
}

func (b *Worker) Context() context.Context {
	b.init()

	return b.ctx
}

func (b *Worker) Start(name string, f func(ctx context.Context)) {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	if b.ctx.Err() != nil {
		return
	}

	b.workers.Add(1)
	go func() {
		defer b.workers.Done()

		pprof.Do(b.ctx, pprof.Labels("background", name), f)
	}()
}

func (b *Worker) Done() <-chan struct{} {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	return b.ctx.Done()
}

func (b *Worker) Close(ctx context.Context, err error) error {
	b.init()

	b.stop(err)

	waitCtx, waitCancel := context.WithCancel(ctx)

	go func() {
		b.workers.Wait()
		waitCancel()
	}()

	<-waitCtx.Done()
	return ctx.Err()
}

func (b *Worker) init() {
	b.onceInit.Do(func() {
		if b.ctx == nil {
			b.ctx, b.stop = xcontext.WithErrCancel(context.Background())
		}
	})
}
