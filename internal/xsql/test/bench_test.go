package test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

const (
	sessionPoolSize = 600
	parallelism     = 100
)

func benchmarkDatabaseSQLSelect42(b *testing.B, nativeDriver *ydb.Driver, useQueryService bool) {
	b.Helper()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(useQueryService),
	)
	require.NoError(b, err)

	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
	}()

	db.SetMaxOpenConns(sessionPoolSize)
	db.SetMaxIdleConns(sessionPoolSize)

	warmUpMock(b.Context(), b, db)

	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var (
			v    int
			rows *sql.Rows
			err  error
		)

		for pb.Next() {
			func() {
				rows, err = db.QueryContext(b.Context(), `SELECT 42`)
				if !assert.NoError(b, err) {
					return
				}
				defer func() {
					assert.NoError(b, rows.Close())
				}()

				for rows.Next() {
					if !assert.NoError(b, rows.Scan(&v)) {
						return
					}
					if !assert.Equal(b, 42, v) {
						return
					}
					v = 0
				}

				assert.NoError(b, rows.Err())
			}()
		}
	})
}

// cpu: Apple M3 Pro
// go test -bench=. -benchtime=10s .
//
// BenchmarkDatabaseSQL/over/QueryService-12		10475 ns/op     22580 B/op      371 allocs/op
// BenchmarkDatabaseSQL/over/TableService-12		9493 ns/op      19131 B/op      306 allocs/op
// Diff (query/table*100-100)						10%				18%				21%
func BenchmarkDatabaseSQL(b *testing.B) {
	ctx := b.Context()

	mockSrv := mock.Server(b)

	nativeDriver, err := ydb.Open(ctx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithSessionPoolSizeLimit(sessionPoolSize),
	)
	require.NoError(b, err)

	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	b.Run("over", func(b *testing.B) {
		for _, engine := range []struct {
			name            string
			useQueryService bool
		}{
			{
				name:            "QueryService",
				useQueryService: true,
			},
			{
				name:            "TableService",
				useQueryService: false,
			},
		} {
			b.Run(engine.name, func(b *testing.B) {
				benchmarkDatabaseSQLSelect42(b, nativeDriver, engine.useQueryService)
			})
		}
	})
}

func warmUpMock(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()

	wg := sync.WaitGroup{}
	wg.Add(sessionPoolSize)

	for range sessionPoolSize {
		go func() {
			defer wg.Done()
			rows, err := db.QueryContext(ctx, `SELECT 42`)
			if !assert.NoError(t, err) {
				return
			}
			defer func() {
				assert.NoError(t, rows.Close())
			}()

			var v int

			for rows.Next() {
				if !assert.NoError(t, rows.Scan(&v)) {
					return
				}
			}

			assert.NoError(t, rows.Err())
		}()
	}

	wg.Wait()
}
