//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func BenchmarkQuery_Query_AllowImplicitSessions(b *testing.B) {
	benchOverQueryService(context.TODO(), b,
		ydb.WithQueryConfigOption(query.AllowImplicitSessions()),
	)
}

func BenchmarkQuery_Query(b *testing.B) {
	benchOverQueryService(context.TODO(), b)
}

func benchOverQueryService(ctx context.Context, b *testing.B, driverOpts ...ydb.Option) {
	b.StopTimer()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local", driverOpts...)
	require.NoError(b, err)
	defer db.Close(ctx)

	q := db.Query()
	const statement = `SELECT 42 as id, "my string" as myStr`
	parallelismValues := []int{1, 2, 16, 128, 512}

	for _, parallelism := range parallelismValues {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			b.StopTimer()
			workers := min(parallelism*runtime.GOMAXPROCS(0), b.N)
			start := make(chan struct{})
			var ready, done sync.WaitGroup
			var next atomic.Int64
			errors := make(chan error, workers)
			ready.Add(workers)
			done.Add(workers)
			for range workers {
				go func() {
					defer done.Done()
					result, err := q.Query(ctx, statement)
					if err == nil {
						err = result.Close(ctx)
					}
					if err != nil {
						errors <- err
					}
					ready.Done()
					<-start
					if err != nil {
						return
					}
					for next.Add(1) <= int64(b.N) {
						result, err := q.Query(ctx, statement)
						if err == nil {
							err = result.Close(ctx)
						}
						if err != nil {
							errors <- err
							return
						}
					}
				}()
			}
			ready.Wait()
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			close(start)
			done.Wait()
			b.StopTimer()
			close(errors)
			for err := range errors {
				b.Fatalf("query benchmark failed: %v", err)
			}
		})
	}
}
