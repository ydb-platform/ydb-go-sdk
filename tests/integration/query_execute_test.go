//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryExecute(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithTraceQuery(
			log.Query(
				log.Default(os.Stdout,
					log.WithLogQuery(),
					log.WithColoring(),
					log.WithMinLevel(log.INFO),
				),
				trace.QueryEvents,
			),
		),
	)
	require.NoError(t, err)
	t.Run("Execute", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		result, err := db.Query().Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT $p1, $p2, $p3;
				`,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$p1").Text("test").
					Param("$p2").Uint64(100500000000).
					Param("$p3").Interval(time.Duration(100500000000)).
					Build(),
			),
			query.WithSyntax(query.SyntaxYQL),
			query.WithStatsMode(query.StatsModeFull),
		)
		require.NoError(t, err)
		resultSet, err := result.NextResultSet(ctx)
		require.NoError(t, err)
		row, err := resultSet.NextRow(ctx)
		require.NoError(t, err)
		err = row.Scan(&p1, &p2, &p3)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
		t.Run("Stats", func(t *testing.T) {
			s := result.Stats()
			require.NotNil(t, s)
			t.Logf("Stats: %+v", s)
			require.NotZero(t, s.QueryAST())
			require.NotZero(t, s.QueryPlan())
			require.NotZero(t, s.TotalDuration)
			require.NotZero(t, s.TotalCPUTime)
			require.NotZero(t, s.ProcessCPUTime)
			require.NotZero(t, s.Compilation)
			_, ok := s.NextPhase()
			require.True(t, ok)
		})
	})
	t.Run("Stats", func(t *testing.T) {
		t.Run("Client", func(t *testing.T) {
			s := db.Query().Stats()
			require.EqualValues(t, -1, s.Limit)
			t.Run("Do", func(t *testing.T) {
				t.Run("Execute", func(t *testing.T) {
					err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
						_, result, err := s.Execute(ctx, `SELECT 123`,
							query.WithStatsMode(query.StatsModeFull),
						)
						if err != nil {
							return err
						}
						resultSet, err := result.NextResultSet(ctx)
						if err != nil {
							return err
						}
						row, err := resultSet.NextRow(ctx)
						if err != nil {
							return err
						}
						var v int32
						err = row.Scan(&v)
						if err != nil {
							return err
						}
						if v != 123 {
							return fmt.Errorf("unexpected value from database: %d", v)
						}
						queryStats := result.Stats()
						if err != nil {
							return err
						}
						if queryStats == nil {
							return fmt.Errorf("unexpected nil query stats")
						}
						if queryStats.QueryPlan() == "" {
							return fmt.Errorf("unexpected empty plan")
						}
						if queryStats.QueryAST() == "" {
							return fmt.Errorf("unexpected empty AST")
						}
						if queryStats.ProcessCPUTime() == 0 {
							return fmt.Errorf("unexpected process CPU time: %d", queryStats.ProcessCPUTime())
						}
						if queryStats.TotalDuration() == 0 {
							return fmt.Errorf("unexpected total duration: %d", queryStats.TotalDuration())
						}
						if queryStats.TotalCPUTime() == 0 {
							return fmt.Errorf("unexpected total CPU time: %d", queryStats.TotalCPUTime())
						}
						if _, ok := queryStats.NextPhase(); !ok {
							return fmt.Errorf("unexpected empty query phases")
						}
						return result.Err()
					}, query.WithIdempotent())
					require.NoError(t, err)
				})
			})
			t.Run("DoTx", func(t *testing.T) {
				t.Run("Execute", func(t *testing.T) {
					err = db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) (err error) {
						result, err := tx.Execute(ctx, `SELECT 123`,
							query.WithStatsMode(query.StatsModeFull),
						)
						if err != nil {
							return err
						}
						resultSet, err := result.NextResultSet(ctx)
						if err != nil {
							return err
						}
						row, err := resultSet.NextRow(ctx)
						if err != nil {
							return err
						}
						var v int32
						err = row.Scan(&v)
						if err != nil {
							return err
						}
						if v != 123 {
							return fmt.Errorf("unexpected value from database: %d", v)
						}
						queryStats := result.Stats()
						if err != nil {
							return err
						}
						if queryStats == nil {
							return fmt.Errorf("unexpected nil query stats")
						}
						if queryStats.QueryPlan() == "" {
							return fmt.Errorf("unexpected empty plan")
						}
						if queryStats.QueryAST() == "" {
							return fmt.Errorf("unexpected empty AST")
						}
						if queryStats.ProcessCPUTime() == 0 {
							return fmt.Errorf("unexpected process CPU time: %d", queryStats.ProcessCPUTime())
						}
						if queryStats.TotalDuration() == 0 {
							return fmt.Errorf("unexpected total duration: %d", queryStats.TotalDuration())
						}
						if queryStats.TotalCPUTime() == 0 {
							return fmt.Errorf("unexpected total CPU time: %d", queryStats.TotalCPUTime())
						}
						if _, ok := queryStats.NextPhase(); !ok {
							return fmt.Errorf("unexpected empty query phases")
						}
						return result.Err()
					}, query.WithIdempotent())
					require.NoError(t, err)
				})
			})
			t.Run("Execute", func(t *testing.T) {
				result, err := db.Query().Execute(ctx, `SELECT 123`,
					query.WithStatsMode(query.StatsModeFull),
				)
				require.NoError(t, err)
				resultSet, err := result.NextResultSet(ctx)
				require.NoError(t, err)
				row, err := resultSet.NextRow(ctx)
				require.NoError(t, err)
				var v int32
				err = row.Scan(&v)
				require.NoError(t, err)
				require.EqualValues(t, 123, v)
				queryStats := result.Stats()
				require.NoError(t, err)
				require.NotNil(t, queryStats)
				require.NotZero(t, queryStats.QueryPlan())
				require.NotZero(t, queryStats.QueryAST())
				require.NotZero(t, queryStats.ProcessCPUTime())
				require.NotZero(t, queryStats.TotalDuration())
				require.NotZero(t, queryStats.TotalCPUTime())
				_, ok := queryStats.NextPhase()
				require.True(t, ok)
			})
		})
	})
	t.Run("Scan", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, result, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT $p1, $p2, $p3;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := resultSet.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.Scan(&p1, &p2, &p3)
			if err != nil {
				return err
			}
			return result.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})
	t.Run("ScanNamed", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, result, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT $p1 AS p1, $p2 AS p2, $p3 AS p3;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := resultSet.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.ScanNamed(
				query.Named("p1", &p1),
				query.Named("p2", &p2),
				query.Named("p3", &p3),
			)
			if err != nil {
				return err
			}
			return result.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})
	t.Run("ScanStruct", func(t *testing.T) {
		var data struct {
			P1 *string       `sql:"p1"`
			P2 uint64        `sql:"p2"`
			P3 time.Duration `sql:"p3"`
			P4 *string       `sql:"p4"`
		}
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, result, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT CAST($p1 AS Optional<Text>) AS p1, $p2 AS p2, $p3 AS p3, CAST(NULL AS Optional<Text>) AS p4;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := resultSet.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.ScanStruct(&data)
			if err != nil {
				return err
			}
			return result.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.NotNil(t, data.P1)
		require.EqualValues(t, "test", *data.P1)
		require.EqualValues(t, 100500000000, data.P2)
		require.EqualValues(t, time.Duration(100500000000), data.P3)
		require.Nil(t, data.P4)
	})
	t.Run("Tx", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
				tx, err := s.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
				if err != nil {
					return err
				}
				result, err := tx.Execute(ctx, `SELECT 1`)
				if err != nil {
					return err
				}
				resultSet, err := result.NextResultSet(ctx)
				if err != nil {
					return err
				}
				row, err := resultSet.NextRow(ctx)
				if err != nil {
					return err
				}
				var v int32
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 1 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				if err = result.Err(); err != nil {
					return err
				}
				return tx.CommitTx(ctx)
			}, query.WithIdempotent())
			require.NoError(t, err)
		})
		t.Run("Lazy", func(t *testing.T) {
			err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
				tx, result, err := s.Execute(ctx, `SELECT 1`,
					query.WithTxControl(query.TxControl(query.BeginTx(query.WithSerializableReadWrite()))),
				)
				if err != nil {
					return err
				}
				resultSet, err := result.NextResultSet(ctx)
				if err != nil {
					return err
				}
				row, err := resultSet.NextRow(ctx)
				if err != nil {
					return err
				}
				var v int32
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 1 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				if err = result.Err(); err != nil {
					return err
				}
				result, err = tx.Execute(ctx, `SELECT 2`, query.WithCommit())
				if err != nil {
					return err
				}
				resultSet, err = result.NextResultSet(ctx)
				if err != nil {
					return err
				}
				row, err = resultSet.NextRow(ctx)
				if err != nil {
					return err
				}
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 2 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				return result.Err()
			}, query.WithIdempotent())
			require.NoError(t, err)
		})
	})
}
