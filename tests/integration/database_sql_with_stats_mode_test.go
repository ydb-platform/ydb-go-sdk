//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestDatabaseSqlWithStatsMode(t *testing.T) {
	engines := []struct {
		name         string
		queryService bool
	}{
		{"QueryService", true},
		{"TableService", false},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			scope := newScope(t)
			tableName := scope.TableName()

			db := scope.SQLDriverWithFolder(ydb.WithQueryService(engine.queryService))
			defer db.Close()

			t.Run("Basic", func(t *testing.T) {
				t.Run("Exec", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeBasic(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 1, qs)
					})

					_, err := db.ExecContext(ctx,
						"UPSERT INTO `"+tableName+"` (id, val) VALUES (1, 'basic-exec')",
					)
					require.NoError(t, err)
					require.True(t, callbackCalled.Load(), "stats callback must be called for Exec with StatsModeBasic")
				})

				t.Run("Query", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeBasic(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 0, qs)
					})

					rows, err := db.QueryContext(ctx, "SELECT id, val FROM `"+tableName+"`")
					require.NoError(t, err)

					for rows.Next() {
						var id int64
						var val sql.NullString
						require.NoError(t, rows.Scan(&id, &val))
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())

					require.True(t, callbackCalled.Load(), "stats callback must be called for Query with StatsModeBasic")
				})
			})

			t.Run("Full", func(t *testing.T) {
				t.Run("Exec", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeFull(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 1, qs)
					})

					result, err := db.ExecContext(ctx,
						"UPSERT INTO `"+tableName+"` (id, val) VALUES (2, 'full-exec')",
					)
					require.NoError(t, err)
					require.True(t, callbackCalled.Load(), "stats callback must be called for Exec with StatsModeFull")

					if engine.queryService {
						rowsAffected, err := result.RowsAffected()
						require.NoError(t, err)
						require.Equal(t, int64(1), rowsAffected,
							"RowsAffected must be available with StatsModeFull")
					}
				})

				t.Run("Query", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeFull(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 0, qs)
					})

					rows, err := db.QueryContext(ctx, "SELECT id, val FROM `"+tableName+"`")
					require.NoError(t, err)

					for rows.Next() {
						var id int64
						var val sql.NullString
						require.NoError(t, rows.Scan(&id, &val))
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())

					require.True(t, callbackCalled.Load(), "stats callback must be called for Query with StatsModeFull")
				})
			})

			t.Run("Profile", func(t *testing.T) {
				t.Run("Exec", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeProfile(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 1, qs)
					})

					result, err := db.ExecContext(ctx,
						"UPSERT INTO `"+tableName+"` (id, val) VALUES (3, 'profile-exec')",
					)
					require.NoError(t, err)
					require.True(t, callbackCalled.Load(), "stats callback must be called for Exec with StatsModeProfile")

					if engine.queryService {
						rowsAffected, err := result.RowsAffected()
						require.NoError(t, err)
						require.EqualValues(t, 1, rowsAffected,
							"RowsAffected must be available with StatsModeProfile")
					}
				})

				t.Run("Query", func(t *testing.T) {
					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeProfile(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
						assertUpdates(t, 0, qs)
					})

					rows, err := db.QueryContext(ctx, "SELECT id, val FROM `"+tableName+"`")
					require.NoError(t, err)

					for rows.Next() {
						var id int64
						var val sql.NullString
						require.NoError(t, rows.Scan(&id, &val))
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())

					require.True(t, callbackCalled.Load(), "stats callback must be called for Query with StatsModeProfile")
				})
			})

			t.Run("Transaction", func(t *testing.T) {
				t.Run("Full", func(t *testing.T) {
					tx, err := db.BeginTx(scope.Ctx, nil)
					require.NoError(t, err)

					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeFull(scope.Ctx, func(qs query.Stats) {
						callbackCalled.Store(true)
					})

					_, err = tx.ExecContext(ctx,
						"UPSERT INTO `"+tableName+"` (id, val) VALUES (4, 'tx-full-exec')",
					)
					require.NoError(t, err)
					require.True(t, callbackCalled.Load(), "stats callback must be called for tx Exec with StatsModeFull")

					callbackCalled.Store(false)
					rows, err := tx.QueryContext(ctx, "SELECT id, val FROM `"+tableName+"`")
					require.NoError(t, err)

					for rows.Next() {
						var id int64
						var val sql.NullString
						require.NoError(t, rows.Scan(&id, &val))
					}
					require.NoError(t, rows.Err())
					require.NoError(t, rows.Close())

					require.True(t, callbackCalled.Load(),
						"stats callback must be called for tx Query with StatsModeFull")

					require.NoError(t, tx.Commit())
				})

				t.Run("Profile", func(t *testing.T) {
					tx, err := db.BeginTx(scope.Ctx, nil)
					require.NoError(t, err)

					var callbackCalled atomic.Bool
					ctx := ydb.WithStatsModeProfile(scope.Ctx, func(s query.Stats) {
						callbackCalled.Store(true)
					})

					_, err = tx.ExecContext(ctx,
						"UPSERT INTO `"+tableName+"` (id, val) VALUES (5, 'tx-profile-exec')",
					)
					require.NoError(t, err)
					require.True(t, callbackCalled.Load(), "stats callback must be called for tx Exec with StatsModeProfile")

					require.NoError(t, tx.Commit())
				})
			})
		})
	}
}

func assertUpdates(t *testing.T, expected uint64, qs query.Stats) {
	t.Helper()

	var rowsAffected uint64
	for queryPhase := range qs.QueryPhases() {
		for tableAccess := range queryPhase.TableAccess() {
			rowsAffected += tableAccess.Updates.Rows
		}
	}

	assert.Equal(t, expected, rowsAffected)
}
