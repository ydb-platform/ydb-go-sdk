package pool

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

func newBenchPool(ctx context.Context) (*Pool[*testItem, testItem], error) {
	var created atomic.Uint64

	p, err := New[*testItem, testItem](ctx,
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
		WithWarmUpItems[*testItem, testItem](benchPrefillItems),
	)
	if err != nil {
		return nil, err
	}

	return p, nil
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

func benchPoolWithOnce(
	ctx context.Context,
	p *Pool[*testItem, testItem],
	ops *atomic.Uint64,
) error {
	return p.With(ctx, func(context.Context, *testItem) error {
		return benchPoolWithWork(ops)
	}, benchRetryOpts...)
}

func reportBenchLatency(b *testing.B, samples []uint64) {
	b.Helper()

	if len(samples) == 0 {
		return
	}

	slices.Sort(samples)

	n := len(samples)
	p50 := samples[n*50/100]
	p99Idx := n * 99 / 100
	if p99Idx >= n {
		p99Idx = n - 1
	}
	p99 := samples[p99Idx]

	b.ReportMetric(float64(p50), "ns/op(p50)")
	b.ReportMetric(float64(p99), "ns/op(p99)")
}

func benchmarkPoolWithConcurrency(b *testing.B, goroutines int) {
	b.Helper()

	ctx := b.Context()
	p, err := newBenchPool(ctx)
	if err != nil {
		b.Fatalf("new pool: %v", err)
	}
	defer func() {
		_ = p.Close(ctx)
	}()

	var ops atomic.Uint64

	if goroutines == 1 {
		samples := make([]uint64, b.N)

		b.ResetTimer()
		b.ReportAllocs()
		for i := range b.N {
			start := time.Now()
			if err := benchPoolWithOnce(ctx, p, &ops); err != nil {
				b.Fatalf("pool.With: %v", err)
			}
			samples[i] = uint64(time.Since(start))
		}
		reportBenchLatency(b, samples)

		return
	}

	perWorker := b.N / goroutines
	extra := b.N % goroutines

	samples := make([][]uint64, goroutines)
	for g := range goroutines {
		iterations := perWorker
		if g < extra {
			iterations++
		}
		samples[g] = make([]uint64, iterations)
	}

	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)
	wg.Add(goroutines)
	start := make(chan struct{})

	for g := range goroutines {
		go func(local []uint64) {
			defer wg.Done()
			<-start
			for i := range local {
				t0 := time.Now()
				if err := benchPoolWithOnce(ctx, p, &ops); err != nil {
					errOnce.Do(func() { firstErr = err })

					return
				}
				local[i] = uint64(time.Since(t0))
			}
		}(samples[g])
	}

	b.ResetTimer()
	b.ReportAllocs()
	close(start)
	wg.Wait()
	if firstErr != nil {
		b.Fatalf("pool.With: %v", firstErr)
	}

	merged := make([]uint64, 0, b.N)
	for _, part := range samples {
		merged = append(merged, part...)
	}
	reportBenchLatency(b, merged)
}

// benchmark name                         CPU time   p50 (ns/op)    p99 (ns/op)    B/op   allocs/op
// BenchmarkPoolWith/concurrency=1-12     1049       500.0          10708          974    19
// BenchmarkPoolWith/concurrency=250-12   649.0      1083           149542         982    19
// BenchmarkPoolWith/concurrency=500-12   645.8      1083           152000         982    19
// BenchmarkPoolWith/concurrency=1000-12  1345       515042         1672916        982    19
func BenchmarkPoolWith(b *testing.B) {
	for _, goroutines := range []int{1, 250, 500, 1000} {
		b.Run(fmt.Sprintf("concurrency=%d", goroutines), func(b *testing.B) {
			benchmarkPoolWithConcurrency(b, goroutines)
		})
	}
}
