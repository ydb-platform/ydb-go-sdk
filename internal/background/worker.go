package background

import (
	"context"
	"errors"
	"runtime/pprof"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	ErrAlreadyClosed       = xerrors.Wrap(errors.New("ydb: background worker already closed"))
	errClosedWithNilReason = xerrors.Wrap(errors.New("ydb: background worker closed with nil reason"))
)

// A Worker must not be copied after first use
type Worker struct {
	ctx      context.Context
	workers  sync.WaitGroup
	onceInit sync.Once

	m xsync.Mutex

	closed      uint32
	stop        xcontext.CancelErrFunc
	closeReason error
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
	if atomic.LoadUint32(&b.closed) != 0 {
		f(b.ctx)
		return
	}

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
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		return xerrors.WithStackTrace(ErrAlreadyClosed)
	}

	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	b.closeReason = err
	if b.closeReason == nil {
		b.closeReason = errClosedWithNilReason
	}

	b.stop(err)

	b.workers.Wait()

	return ctx.Err()
}

func (b *Worker) CloseReason() error {
	b.m.Lock()
	defer b.m.Unlock()

	return b.closeReason
}

func (b *Worker) init() {
	b.onceInit.Do(func() {
		if b.ctx == nil {
			b.ctx, b.stop = xcontext.WithErrCancel(context.Background())
		}
	})
}
