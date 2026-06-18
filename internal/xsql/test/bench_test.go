package test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

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
// BenchmarkDatabaseSQL/over/QueryService-12	11109 ns/op		22242 B/op		363 allocs/op
// BenchmarkDatabaseSQL/over/TableService-12	9411 ns/op		19140 B/op		306 allocs/op
// Diff (query/table*100-100)					18%				16%				18%
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

const (
	prefetchBenchParts       = 8
	prefetchBenchNetDelay    = 200 * time.Microsecond
	prefetchBenchWorkBetween = 300 * time.Microsecond
)

func prefetchBenchSQL(parts int) string {
	queries := make([]string, parts)
	for i := range parts {
		queries[i] = "SELECT " + strconv.Itoa(i+1)
	}

	return strings.Join(queries, "; ")
}

func drainMultiResultSetRows(rows *sql.Rows, workBetween time.Duration) error {
	for rsIdx := 0; ; rsIdx++ {
		if rsIdx > 0 {
			if !rows.NextResultSet() {
				break
			}
		}

		for rows.Next() {
			cols, err := rows.Columns()
			if err != nil {
				return err
			}

			switch len(cols) {
			case 1:
				var v int32
				if err := rows.Scan(&v); err != nil {
					return err
				}
			case 2:
				var (
					v       int32
					payload []byte
				)
				if err := rows.Scan(&v, &payload); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unexpected column count: %d", len(cols))
			}

			if workBetween > 0 {
				time.Sleep(workBetween)
			}
		}
	}

	return rows.Err()
}

func benchmarkDatabaseSQLPrefetchQueryResultParts(b *testing.B, nativeDriver *ydb.Driver, prefetchParts int) {
	b.Helper()

	connector, err := ydb.Connector(nativeDriver,
		ydb.WithQueryService(true),
		ydb.WithPrefetchQueryResultParts(prefetchParts),
	)
	require.NoError(b, err)

	defer func() {
		_ = connector.Close()
	}()

	db := sql.OpenDB(connector)
	defer func() {
		_ = db.Close()
	}()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	warmUpMockPrefetchBench(b.Context(), b, db)

	query := prefetchBenchSQL(prefetchBenchParts)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		rows, err := db.QueryContext(b.Context(), query)
		if err != nil {
			b.Fatal(err)
		}

		if err := drainMultiResultSetRows(rows, prefetchBenchWorkBetween); err != nil {
			_ = rows.Close()
			b.Fatal(err)
		}

		if err := rows.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func warmUpMockPrefetchBench(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()

	query := prefetchBenchSQL(prefetchBenchParts)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		rows, err := db.QueryContext(ctx, query)
		if !assert.NoError(t, err) {
			return
		}
		defer func() {
			assert.NoError(t, rows.Close())
		}()

		assert.NoError(t, drainMultiResultSetRows(rows, 0))
	}()

	wg.Wait()
}

// BenchmarkDatabaseSQLPrefetchQueryResultParts compares database/sql Query Service
// throughput when draining a multi-part ExecuteQuery stream with and without
// response-part prefetch enabled on the connector.
//
// The mock server paces stream parts with a per-part delay and large payloads so
// gRPC flow control prevents buffering the whole stream ahead of the client.
//
// cpu: Apple M3 Pro
// go test -bench=BenchmarkDatabaseSQLPrefetchQueryResultParts -benchtime=3s ./internal/xsql/test
//
// BenchmarkDatabaseSQLPrefetchQueryResultParts/no_prefetch-12          985   3716708 ns/op
// BenchmarkDatabaseSQLPrefetchQueryResultParts/prefetch_1-12          1065   3349349 ns/op
func BenchmarkDatabaseSQLPrefetchQueryResultParts(b *testing.B) {
	ctx := b.Context()

	mockSrv := mock.Server(b, mock.WithExecuteQueryPartDelay(prefetchBenchNetDelay))

	nativeDriver, err := ydb.Open(ctx, mockSrv.ConnString(),
		ydb.WithAnonymousCredentials(),
		ydb.WithSessionPoolSizeLimit(sessionPoolSize),
	)
	require.NoError(b, err)

	defer func() {
		_ = nativeDriver.Close(ctx)
	}()

	for _, tc := range []struct {
		name          string
		prefetchParts int
	}{
		{name: "no_prefetch", prefetchParts: 0},
		{name: "prefetch_1", prefetchParts: 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkDatabaseSQLPrefetchQueryResultParts(b, nativeDriver, tc.prefetchParts)
		})
	}
}
