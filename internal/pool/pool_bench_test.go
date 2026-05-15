package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

const (
	benchPoolLimit         = 500
	benchPrefillItems      = benchPoolLimit / 3
	benchDeleteProbability = 20 // 1/N operations triggers item delete and recreate
	benchCreateItemTimeout = time.Second
	benchCloseItemTimeout  = time.Second
)

var errBenchDeleteItem = errors.New("bench: delete pool item")

func newBenchPool(ctx context.Context) *Pool[*testItem, testItem] {
	var created atomic.Uint64

	return New[*testItem, testItem](ctx,
		WithLimit[*testItem, testItem](benchPoolLimit),
		WithCreateItemTimeout[*testItem, testItem](benchCreateItemTimeout),
		WithCloseItemTimeout[*testItem, testItem](benchCloseItemTimeout),
		WithCreateItemFunc(func(context.Context) (*testItem, error) {
			id := created.Add(1)

			return &testItem{v: int32(id)}, nil
		}),
		WithMustDeleteItemFunc[*testItem, testItem](func(_ *testItem, err error) bool {
			return errors.Is(err, errBenchDeleteItem)
		}),
	)
}

func prefillBenchPool(ctx context.Context, p *Pool[*testItem, testItem], count int) error {
	items := make([]*testItem, 0, count)
	for range count {
		item, err := p.getItem(ctx)
		if err != nil {
			for _, acquired := range items {
				p.closeItem(ctx, acquired,
					closeItemWithLock(),
					closeItemWithDeleteFromPool(),
				)
			}

			return err
		}
		items = append(items, item)
	}
	for _, item := range items {
		if err := p.putItem(ctx, item); err != nil {
			return err
		}
	}

	return nil
}

var benchRetryOpts = []retry.Option{
	retry.WithFastBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
	retry.WithSlowBackoff(backoff.New(backoff.WithSlotDuration(time.Nanosecond))),
}

func benchPoolWithWork(ops *atomic.Uint64) error {
	if ops.Add(1)%benchDeleteProbability == 0 {
		return xerrors.Retryable(errBenchDeleteItem)
	}

	return nil
}

func benchmarkPoolWithConcurrency(b *testing.B, goroutines int) {
	b.Helper()

	ctx := b.Context()
	p := newBenchPool(ctx)
	if err := prefillBenchPool(ctx, p, benchPrefillItems); err != nil {
		b.Fatalf("prefill pool: %v", err)
	}
	defer func() {
		_ = p.Close(ctx)
	}()

	var ops atomic.Uint64
	work := func() error {
		return p.With(ctx, func(context.Context, *testItem) error {
			return benchPoolWithWork(&ops)
		}, benchRetryOpts...)
	}

	if goroutines == 1 {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			if err := work(); err != nil {
				b.Fatalf("pool.With: %v", err)
			}
		}

		return
	}

	perWorker := b.N / goroutines
	extra := b.N % goroutines

	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)
	wg.Add(goroutines)
	start := make(chan struct{})

	for g := range goroutines {
		iterations := perWorker
		if g < extra {
			iterations++
		}
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				if err := work(); err != nil {
					errOnce.Do(func() { firstErr = err })

					return
				}
			}
		}()
	}

	b.ResetTimer()
	b.ReportAllocs()
	close(start)
	wg.Wait()
	if firstErr != nil {
		b.Fatalf("pool.With: %v", firstErr)
	}
}

// BenchmarkPoolWith/concurrency=1-12         	 1000000	      1303 ns/op	    1067 B/op	      21 allocs/op
// BenchmarkPoolWith/concurrency=250-12       	  816691	      1327 ns/op	    1070 B/op	      21 allocs/op
// BenchmarkPoolWith/concurrency=500-12       	  770821	      1307 ns/op	    1068 B/op	      21 allocs/op
// BenchmarkPoolWith/concurrency=1000-12      	  779068	      2282 ns/op	    1623 B/op	      29 allocs/op
func BenchmarkPoolWith(b *testing.B) {
	for _, goroutines := range []int{1, 250, 500, 1000} {
		b.Run(fmt.Sprintf("concurrency=%d", goroutines), func(b *testing.B) {
			benchmarkPoolWithConcurrency(b, goroutines)
		})
	}
}
