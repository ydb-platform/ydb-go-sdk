package background

import (
	"context"
	"errors"
	"runtime/pprof"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
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

	tasksCompleted empty.Chan

	m xsync.Mutex

	tasks chan backgroundTask

	closed      bool
	stop        context.CancelFunc
	closeReason error
}

type CallbackFunc func(ctx context.Context)

func NewWorker(parent context.Context) *Worker {
	w := Worker{}
	w.ctx, w.stop = xcontext.WithCancel(parent)

	return &w
}

func (b *Worker) Context() context.Context {
	b.init()

	return b.ctx
}

func (b *Worker) Start(name string, f CallbackFunc) {
	b.init()

	b.m.WithLock(func() {
		if b.closed {
			return
		}

		b.tasks <- backgroundTask{
			callback: f,
			name:     name,
		}
	})
}

func (b *Worker) Done() <-chan struct{} {
	b.init()

	return b.ctx.Done()
}

func (b *Worker) Close(ctx context.Context, err error) error {
	b.init()

	var resErr error
	b.m.WithLock(func() {
		if b.closed {
			resErr = xerrors.WithStackTrace(ErrAlreadyClosed)
			return
		}

		b.closed = true

		close(b.tasks)
		b.closeReason = err
		if b.closeReason == nil {
			b.closeReason = errClosedWithNilReason
		}

		b.stop()
	})
	if resErr != nil {
		return resErr
	}

	<-b.tasksCompleted

	bgCompleted := make(empty.Chan)

	go func() {
		b.workers.Wait()
		close(bgCompleted)
	}()

	select {
	case <-bgCompleted:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Worker) CloseReason() error {
	b.m.Lock()
	defer b.m.Unlock()

	return b.closeReason
}

func (b *Worker) init() {
	b.onceInit.Do(func() {
		if b.ctx == nil {
			b.ctx, b.stop = xcontext.WithCancel(context.Background())
		}
		b.tasks = make(chan backgroundTask)
		b.tasksCompleted = make(empty.Chan)
		go b.starterLoop()
	})
}

func (b *Worker) starterLoop() {
	defer close(b.tasksCompleted)

	for bgTask := range b.tasks {
		b.workers.Add(1)

		go func(task backgroundTask) {
			defer b.workers.Done()

			pprof.Do(b.ctx, pprof.Labels("background", task.name), task.callback)
		}(bgTask)
	}
}

type backgroundTask struct {
	callback CallbackFunc
	name     string
}
