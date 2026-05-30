package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
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

			err := driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				_, res, err := s.Execute(ctx, table.DefaultTxControl(), `SELECT 42`, nil)
				if err != nil {
					return err
				}
				defer func() {
					_ = res.Close()
				}()

				if err = res.NextResultSetErr(ctx); err != nil {
					return err
				}

				var v int32

				for res.NextRow() {
					if err = res.Scan(indexed.Required(&v)); err != nil {
						return err
					}
					if v != 42 {
						return fmt.Errorf("unexpected value %d", v)
					}
				}

				return res.Err()
			}, table.WithIdempotent())
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
}

// BenchmarkTable measures table.Client (driver.Table()) against the in-process xsql mock: Session.Execute("SELECT 42").
//
// BenchmarkTable/over_query-client-12      10264 ns/op    20903 B/op   352 allocs/op
// BenchmarkTable/original_table-client-12  11132 ns/op    19877 B/op   335 allocs/op
// Diff (query/table*100-100)				-7.8%		   5%			5%
func BenchmarkTable(b *testing.B) {
	ctx := b.Context()

	mockSrv := mock.Server(b)

	for _, tc := range []struct {
		name            string
		overQueryClient bool
	}{
		{name: "over query-client", overQueryClient: true},
		{name: "original table-client", overQueryClient: false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			driver, err := ydb.Open(ctx, mockSrv.ConnString(),
				ydb.WithAnonymousCredentials(),
				ydb.WithSessionPoolSizeLimit(sessionPoolSize),
				ydb.WithExecuteDataQueryOverQueryClient(tc.overQueryClient),
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
						err := driver.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
							_, res, err := s.Execute(ctx, table.DefaultTxControl(), `SELECT 42`, nil)
							if err != nil {
								return err
							}
							defer func() {
								_ = res.Close()
							}()

							if err = res.NextResultSetErr(ctx); err != nil {
								return err
							}

							var v int32

							for res.NextRow() {
								if err = res.Scan(indexed.Required(&v)); err != nil {
									return err
								}
								if v != 42 {
									return fmt.Errorf("unexpected value %d", v)
								}
							}

							return res.Err()
						}, table.WithIdempotent())
						assert.NoError(b, err)
					}()
				}
			})
		})
	}
}
