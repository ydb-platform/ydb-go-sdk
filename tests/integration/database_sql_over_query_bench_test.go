//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const benchmarkDatabaseSQLSessionPoolSize = 600

// Bench results:
//
//	goos: darwin
//	goarch: arm64
//	cpu: Apple M3 Pro
//	BenchmarkDatabaseSQL/QueryService-1-12         	    8871	    136256 ns/op	   23766 B/op	     397 allocs/op
//	BenchmarkDatabaseSQL/QueryService-100-12       	    9988	    105302 ns/op	   26209 B/op	     439 allocs/op
//	BenchmarkDatabaseSQL/TableService-1-12         	   13214	     88301 ns/op	   14361 B/op	     234 allocs/op
//	BenchmarkDatabaseSQL/TableService-100-12       	   18547	     56596 ns/op	   15203 B/op	     249 allocs/op
func BenchmarkDatabaseSQL(b *testing.B) {
	ctx := b.Context()

	nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithSessionPoolSizeLimit(benchmarkDatabaseSQLSessionPoolSize),
	)

	require.NoError(b, err)

	defer nativeDriver.Close(ctx) //nolint:errcheck

	for _, engine := range []struct {
		name            string
		useQueryService bool
	}{
		{
			name:            "QueryService",
			useQueryService: true,
		},
		// {
		// 	name:            "TableService",
		// 	useQueryService: false,
		// },
	} {
		connector, err := ydb.Connector(nativeDriver,
			ydb.WithQueryService(engine.useQueryService),
		)
		require.NoError(b, err)

		defer connector.Close() //nolint:errcheck

		db := sql.OpenDB(connector)
		defer db.Close() //nolint:errcheck

		db.SetMaxOpenConns(benchmarkDatabaseSQLSessionPoolSize)

		warmUp(ctx, b, db)

		for _, parallelism := range []int{1, 100} {
			b.Run(engine.name+"-"+strconv.Itoa(parallelism), func(b *testing.B) {
				b.SetParallelism(parallelism)
				b.RunParallel(func(pb *testing.PB) {
					var (
						v    int
						rows *sql.Rows
						err  error
					)

					for pb.Next() {
						func() {
							rows, err = db.QueryContext(ctx, `SELECT 42`)
							require.NoError(b, err)
							defer func() {
								require.NoError(b, rows.Close())
							}()

							for rows.Next() {
								require.NoError(b, rows.Scan(&v))
								assert.Equal(b, 42, v)
								v = 0
							}

							require.NoError(b, rows.Err())
						}()
					}
				})
			})
		}

	}
}

func warmUp(ctx context.Context, t testing.TB, db *sql.DB) {
	t.Helper()

	wg := sync.WaitGroup{}
	for range benchmarkDatabaseSQLSessionPoolSize {
		wg.Go(func() {
			rows, err := db.QueryContext(ctx, `SELECT 42`)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rows.Close())
			}()
		})
	}

	wg.Wait()
}
