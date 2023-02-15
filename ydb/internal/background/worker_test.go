package background

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xatomic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestWorkerContext(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		w := Worker{}
		require.NotNil(t, w.Context())
		require.NotNil(t, w.ctx)
		require.NotNil(t, w.stop)
	})

	t.Run("Dedicated", func(t *testing.T) {
		type ctxkey struct{}
		ctx := context.WithValue(context.Background(), ctxkey{}, "2")
		w := NewWorker(ctx)
		require.Equal(t, "2", w.Context().Value(ctxkey{}))
	})

	t.Run("Stop", func(t *testing.T) {
		w := Worker{}
		ctx := w.Context()
		require.NoError(t, ctx.Err())

		_ = w.Close(context.Background(), nil)
		require.Error(t, ctx.Err())
	})
}

func TestWorkerStart(t *testing.T) {
	t.Run("Started", func(t *testing.T) {
		w := NewWorker(xtest.Context(t))
		started := make(empty.Chan)
		w.Start("test", func(ctx context.Context) {
			close(started)
		})
		xtest.WaitChannelClosed(t, started)
	})
	t.Run("Stopped", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := NewWorker(ctx)
		_ = w.Close(ctx, nil)

		started := make(empty.Chan)
		w.Start("test", func(ctx context.Context) {
			close(started)
		})

		// expected: no close channel
		time.Sleep(time.Second / 100)
		select {
		case <-started:
			t.Fatal()
		default:
			// pass
		}
	})
}

func TestWorkerClose(t *testing.T) {
	t.Run("StopBackground", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := NewWorker(ctx)

		started := make(empty.Chan)
		stopped := xatomic.Bool{}
		w.Start("test", func(innerCtx context.Context) {
			close(started)
			<-innerCtx.Done()
			stopped.Store(true)
		})

		xtest.WaitChannelClosed(t, started)
		require.NoError(t, w.Close(ctx, nil))
		require.True(t, stopped.Load())
	})

	t.Run("DoubleClose", func(t *testing.T) {
		ctx := xtest.Context(t)
		w := NewWorker(ctx)
		require.NoError(t, w.Close(ctx, nil))
		require.Error(t, w.Close(ctx, nil))
	})
}

func TestWorkerConcurrentStartAndClose(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		targetClose := int64(100)
		parallel := 10

		var counter int64

		ctx := xtest.Context(t)
		w := NewWorker(ctx)

		closeIndex := int64(0)
		closed := make(empty.Chan)

		go func() {
			defer close(closed)

			xtest.SpinWaitCondition(t, nil, func() bool {
				return atomic.LoadInt64(&counter) > targetClose
			})

			require.NoError(t, w.Close(ctx, nil))
			closeIndex = atomic.LoadInt64(&counter)
		}()

		stopNewStarts := xatomic.Bool{}
		for i := 0; i < parallel; i++ {
			go func() {
				for {
					if stopNewStarts.Load() {
						return
					}

					w.Start("test", func(ctx context.Context) {
						atomic.AddInt64(&counter, 1)
					})
				}
			}()
		}

		xtest.WaitChannelClosed(t, closed)
		runtime.Gosched()
		require.Equal(t, closeIndex, atomic.LoadInt64(&counter))
		stopNewStarts.Store(true)
	})
}

func TestWorkerStartCompletedWhileLongWait(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := xtest.Context(t)
		w := NewWorker(ctx)

		allowStop := make(empty.Chan)
		closeStarted := make(empty.Chan)
		w.Start("test", func(ctx context.Context) {
			<-ctx.Done()
			close(closeStarted)

			<-allowStop
		})

		closed := make(empty.Chan)

		callStartFinished := make(empty.Chan)
		go func() {
			defer close(callStartFinished)
			start := time.Now()

			for time.Since(start) < time.Millisecond {
				w.Start("test2", func(ctx context.Context) {
					// pass
				})
			}
		}()

		go func() {
			defer close(closed)

			_ = w.Close(ctx, nil)
		}()

		xtest.WaitChannelClosed(t, callStartFinished)
		runtime.Gosched()

		select {
		case <-closed:
			t.Fatal()
		default:
			// pass
		}

		close(allowStop)
		xtest.WaitChannelClosed(t, closed)
	})
}
