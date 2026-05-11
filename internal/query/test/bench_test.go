package test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

const (
	benchParallelism = 100
	sessionPoolSize  = 600
)

func warmUp(ctx context.Context, t testing.TB, driver *ydb.Driver) {
	t.Helper()

	wg := sync.WaitGroup{}
	wg.Add(sessionPoolSize)

	for range sessionPoolSize {
		go func() {
			defer wg.Done()
			row, err := driver.Query().QueryRow(ctx, `SELECT 42`, query.WithIdempotent())
			if !assert.NoError(t, err) {
				return
			}

			var v int32

			if !assert.NoError(t, row.Scan(&v)) {
				return
			}

			assert.Equal(t, int32(42), v)
		}()
	}

	wg.Wait()
}

// BenchmarkQuery measures query.Client (driver.Query()) against the in-process xsql mock: QueryRow("SELECT 42").
//
// BenchmarkQuery-12  107622    10798 ns/op     23994 B/op    398 allocs/op
func BenchmarkQuery(b *testing.B) {
	ctx := b.Context()

	mockSrv := mock.Server(b)

	driver, err := ydb.Open(ctx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithSessionPoolSizeLimit(sessionPoolSize),
	)
	require.NoError(b, err)

	defer func() {
		_ = driver.Close(ctx)
	}()

	warmUp(ctx, b, driver)

	b.SetParallelism(benchParallelism)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			func() {
				row, err := driver.Query().QueryRow(ctx, `SELECT 42`, query.WithIdempotent())
				if !assert.NoError(b, err) {
					return
				}

				var v int32

				if !assert.NoError(b, row.Scan(&v)) {
					return
				}

				assert.Equal(b, int32(42), v)
			}()
		}
	})
}
