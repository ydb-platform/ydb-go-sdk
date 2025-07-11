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

// BenchmarkQuery_Query_AllowImplicitSessions
// Result:
// goos: darwin
// goarch: arm64
// pkg: github.com/ydb-platform/ydb-go-sdk/v3/tests/integration
// cpu: Apple M3 Pro
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-1-12              7020            193954 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-2-12              7903            173707 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-16-12             7006            156601 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-512-12            6811            165773 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-2048-12           8218            163119 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-16384-12          7093            170583 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-65536-12          6410            176477 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-131072-12         5841            179243 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-262144-12         5552            203478 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-393216-12         3854            274290 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-524288-12            4         251855177 ns/op
// BenchmarkQuery_Query_AllowImplicitSessions/parallel-1048576-12           1        1902566958 ns/op
func BenchmarkQuery_Query_AllowImplicitSessions(b *testing.B) {
	benchOverQueryService(context.TODO(), b,
		ydb.WithQueryConfigOption(query.AllowImplicitSessions()),
	)
}

// BenchmarkQuery_Query
// Result:
// goos: darwin
// goarch: arm64
// pkg: github.com/ydb-platform/ydb-go-sdk/v3/tests/integration
// cpu: Apple M3 Pro
// BenchmarkQuery_Query/parallel-1-12                  9445            128672 ns/op
// BenchmarkQuery_Query/parallel-2-12                 12777            100227 ns/op
// BenchmarkQuery_Query/parallel-16-12                13782             87532 ns/op
// BenchmarkQuery_Query/parallel-512-12               12950             92284 ns/op
// BenchmarkQuery_Query/parallel-2048-12              12091             96659 ns/op
// BenchmarkQuery_Query/parallel-16384-12             14293             94588 ns/op
// BenchmarkQuery_Query/parallel-65536-12             11144             96578 ns/op
// BenchmarkQuery_Query/parallel-131072-12            12848            103441 ns/op
// BenchmarkQuery_Query/parallel-262144-12            10414            120940 ns/op
// BenchmarkQuery_Query/parallel-393216-12             7090            154935 ns/op
// BenchmarkQuery_Query/parallel-524288-12              188           5381982 ns/op
// BenchmarkQuery_Query/parallel-1048576-12               1        1918443542 ns/op
func BenchmarkQuery_Query(b *testing.B) {
	benchOverQueryService(context.TODO(), b)
}

func benchOverQueryService(ctx context.Context, b *testing.B, driverOpts ...ydb.Option) {
	goroutinesCnt := []int{1, 2, 16, 512, 2048, 16384, 65536, 131072, 262144, 393216, 524288, 1048576}

	for _, parallelism := range goroutinesCnt {
		b.Run(fmt.Sprintf("parallel-%d", parallelism), func(b *testing.B) {
			b.SetParallelism(parallelism)

			db, err := ydb.Open(ctx, "grpc://localhost:2136/local", driverOpts...)

			require.NoError(b, err)
			defer db.Close(ctx)

			q := db.Query()

			// Warmup
			_, err = q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
			require.NoError(b, err)

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := q.Query(ctx, `SELECT 42 as id, "my string" as myStr`)
					require.NoError(b, err)
				}
			})
		})
	}
}
