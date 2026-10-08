//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
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
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local", driverOpts...)
	require.NoError(b, err)
	defer db.Close(ctx)

	q := db.Query()

	// Warmup
	_, err = q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
	require.NoError(b, err)

	// RunParallel starts parallelism*GOMAXPROCS goroutines on every calibration run.
	// Keep worker startup small relative to the number of measured queries.
	parallelismValues := []int{1, 2, 16, 128, 512}

	for _, parallelism := range parallelismValues {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
					require.NoError(b, err)
				}
			})
		})
	}
}
