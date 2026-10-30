//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
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

type queryBenchmarkJob struct {
	iterations int64
	next       atomic.Int64
	done       sync.WaitGroup
	errors     chan error
}

func benchOverQueryService(ctx context.Context, b *testing.B, driverOpts ...ydb.Option) {
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local", driverOpts...)
	require.NoError(b, err)
	defer db.Close(ctx)

	q := db.Query()
	const statement = `SELECT 42 as id, "my string" as myStr`
	workerCounts := []int{1, 2, 16, 128, 512}

	for _, workers := range workerCounts {
		b.Run(fmt.Sprintf("workers-%d", workers), func(b *testing.B) {
			jobs := make([]chan *queryBenchmarkJob, workers)
			var ready, workerGroup sync.WaitGroup
			warmupErrors := make(chan error, workers)
			ready.Add(workers)
			workerGroup.Add(workers)
			for i := range jobs {
				jobs[i] = make(chan *queryBenchmarkJob)
				go func(jobChannel <-chan *queryBenchmarkJob) {
					defer workerGroup.Done()
					result, err := q.Query(ctx, statement)
					if err == nil {
						err = result.Close(ctx)
					}
					if err != nil {
						warmupErrors <- err
						ready.Done()
						return
					}
					ready.Done()
					for job := range jobChannel {
						for job.next.Add(1) <= job.iterations {
							result, err := q.Query(ctx, statement)
							if err == nil {
								err = result.Close(ctx)
							}
							if err != nil {
								job.errors <- err
								break
							}
						}
						job.done.Done()
					}
				}(jobs[i])
			}
			ready.Wait()
			close(warmupErrors)
			for err := range warmupErrors {
				for _, jobChannel := range jobs {
					close(jobChannel)
				}
				workerGroup.Wait()
				b.Fatalf("query benchmark warmup failed: %v", err)
			}

			b.Run("query", func(b *testing.B) {
				job := &queryBenchmarkJob{
					iterations: int64(b.N),
					errors:     make(chan error, workers),
				}
				job.done.Add(workers)
				b.ReportAllocs()
				for _, jobChannel := range jobs {
					jobChannel <- job
				}
				job.done.Wait()
				close(job.errors)
				for err := range job.errors {
					b.Fatalf("query benchmark failed: %v", err)
				}
			})
			for _, jobChannel := range jobs {
				close(jobChannel)
			}
			workerGroup.Wait()
		})
	}
}
