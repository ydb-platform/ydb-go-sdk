//go:build integration
// +build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryExecute(t *testing.T) {
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
	t.Run("Query", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		var s query.Stats
		result, err := db.Query().Query(ctx, `
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
			query.WithStatsMode(query.StatsModeFull, func(stats query.Stats) {
				s = stats
			}),
			query.WithIdempotent(),
		)
		require.NoError(t, err)
		resultSet, err := result.NextResultSet(ctx)
		require.NoError(t, err)
		row, err := resultSet.NextRow(ctx)
		require.NoError(t, err)
		err = row.Scan(&p1, &p2, &p3)
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
		t.Run("Stats", func(t *testing.T) {
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
	t.Run("Explain", func(t *testing.T) {
		var (
			ast  string
			plan map[string]any
		)
		err := db.Query().Exec(ctx,
			`SELECT CAST(42 AS Uint32);`,
			query.WithExecMode(query.ExecModeExplain),
			query.WithStatsMode(query.StatsModeNone, func(stats query.Stats) {
				ast = stats.QueryAST()
				err := json.Unmarshal([]byte(stats.QueryPlan()), &plan)
				require.NoError(t, err)
			}),
			query.WithIdempotent(),
		)
		require.NoError(t, err)
		for _, key := range []string{"Plan", "tables", "meta"} {
			_, has := plan[key]
			require.True(t, has, key)
		}
		require.Contains(t, ast, "return")
	})
	t.Run("Scan", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			result, err := s.Query(ctx, `
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
				query.WithIdempotent(),
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
			return nil
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
			result, err := s.Query(ctx, `
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
				query.WithIdempotent(),
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
			return nil
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
			result, err := s.Query(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT CAST($p1 AS Optional<Text>) AS p1, $p2 AS p2, $p3 AS p3, CAST(NULL AS Optional<Text>) AS p4;`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
				query.WithIdempotent(),
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
			return nil
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.NotNil(t, data.P1)
		require.EqualValues(t, "test", *data.P1)
		require.EqualValues(t, 100500000000, data.P2)
		require.EqualValues(t, time.Duration(100500000000), data.P3)
		require.Nil(t, data.P4)
	})
	t.Run("Tx", func(t *testing.T) {
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			tx, err := s.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
			if err != nil {
				return err
			}
			result, err := tx.Query(ctx, `SELECT 1`)
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
			return tx.CommitTx(ctx)
		}, query.WithIdempotent())
		require.NoError(t, err)
	})
}

// https://github.com/ydb-platform/ydb-go-sdk/issues/1456
func TestIssue1456TooManyUnknownTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)

	const (
		tableSize = 10000
		queries   = 1000
		chSize    = 50
	)

	tableName := path.Join(db.Name(), t.Name(), "test")

	err = db.Query().Exec(ctx, "DROP TABLE IF EXISTS `"+tableName+"`;")
	require.NoError(t, err)

	err = db.Query().Exec(ctx, `CREATE TABLE `+"`"+tableName+"`"+` (
			id Utf8,
			value Uint64,
			PRIMARY KEY(id)
			)`,
	)
	require.NoError(t, err)

	var vals []types.Value
	for i := 0; i < tableSize; i++ {
		vals = append(vals, types.StructValue(
			types.StructFieldValue("id", types.UTF8Value(uuid.NewString())),
			types.StructFieldValue("value", types.Uint64Value(rand.Uint64())),
		))
	}
	err = db.Query().Do(context.Background(), func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, `
				PRAGMA AnsiInForEmptyOrNullableItemsCollections;
				DECLARE $vals AS List<Struct<
					id: Utf8,
					value: Uint64
				>>;
				
				INSERT INTO `+"`"+tableName+"`"+` 
				SELECT id, value FROM AS_TABLE($vals);`,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$vals").BeginList().AddItems(vals...).EndList().Build(),
			),
		)
	})
	require.NoError(t, err)

	t.Run("Query", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(queries)
		ch := make(chan struct{}, chSize)
		for i := 0; i < queries; i++ {
			ch <- struct{}{}
			go func() {
				defer func() { <-ch }()
				defer wg.Done()

				err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
					var (
						id string
						v  uint64
					)

					res, err := tx.Query(ctx, `SELECT id, value FROM `+"`"+tableName+"`")
					if err != nil {
						return err
					}

					for {
						set, err := res.NextResultSet(ctx)
						if err != nil {
							if errors.Is(err, io.EOF) {
								break
							}

							return err
						}

						for {
							row, err := set.NextRow(ctx)
							if err != nil {
								if errors.Is(err, io.EOF) {
									break
								}

								return err
							}

							err = row.Scan(&id, &v)
							if err != nil {
								return err
							}
						}
					}
					return res.Close(ctx)
				}, query.WithTxSettings(query.TxSettings(query.WithSerializableReadWrite())))
				require.NoError(t, err)
			}()
		}
		wg.Wait()
	})
}

func TestQueryResultSet(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		scope := newScope(t)

		partSizeBytes := 1000
		targetCount := partSizeBytes * 10 // for guarantee size of response will contain many parts
		items := make([]types.Value, 0, targetCount)
		for i := 0; i < targetCount; i++ {
			item := types.StructValue(
				types.StructFieldValue("val", types.Int64Value(int64(i))),
			)
			items = append(items, item)
		}

		err := scope.Driver().Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
			rs, err := tx.QueryResultSet(ctx, `
DECLARE $arg AS List<Struct<val: Int64>>;

SELECT * FROM AS_TABLE($arg);
`,
				query.WithParameters(ydb.ParamsBuilder().Param("$arg").Any(types.ListValue(items...)).Build()),
				query.WithResponsePartLimitSizeBytes(int64(partSizeBytes)),
			)
			if err != nil {
				return err
			}

			for i := 0; i < targetCount; i++ {
				row, err := rs.NextRow(ctx)
				if err != nil {
					return err
				}

				var val int64
				err = row.Scan(&val)
				require.NoError(t, err)
				require.Equal(t, int64(i), val)
			}

			return nil
		})
		require.NoError(t, err)
	})
	t.Run("FailOnSecondResultSet", func(t *testing.T) {
		scope := newScope(t)

		var secondRowError error
		err := scope.Driver().Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
			rs, err := tx.QueryResultSet(ctx, "SELECT 1; SELECT 2")
			if err != nil {
				return err
			}

			_, err = rs.NextRow(ctx)
			if err != nil {
				return err
			}

			_, secondRowError = rs.NextRow(ctx)

			return nil
		})
		require.NoError(t, err)
		require.Error(t, secondRowError)
		require.NotErrorIs(t, secondRowError, io.EOF)
	})
}

func TestQueryPartLimiter(t *testing.T) {
	if os.Getenv("YDB_VERSION") != "nightly" && version.Lt(os.Getenv("YDB_VERSION"), "25.0") {
		t.Skip("require enables transactions for topics")
	}

	scope := newScope(t)

	var readPartCount int
	scope.Driver(ydb.WithTraceQuery(trace.Query{
		OnResultNextPart: func(info trace.QueryResultNextPartStartInfo) func(info trace.QueryResultNextPartDoneInfo) {
			return func(info trace.QueryResultNextPartDoneInfo) {
				if info.Error == nil {
					readPartCount++
				}
			}
		},
	}))

	targetCount := 1000
	items := make([]types.Value, 0, targetCount)
	for i := 0; i < targetCount; i++ {
		item := types.StructValue(
			types.StructFieldValue("val", types.Int64Value(int64(i))),
		)
		items = append(items, item)
	}

	getPartCount := func(partSize int64) int {
		partCount := 0
		err := scope.Driver().Query().DoTx(scope.Ctx, func(ctx context.Context, tx query.TxActor) error {
			oldParts := readPartCount
			rs, err := tx.QueryResultSet(ctx, `
DECLARE $arg AS List<Struct<val: Int64>>;

SELECT * FROM AS_TABLE($arg);
`,
				query.WithParameters(ydb.ParamsBuilder().Param("$arg").Any(types.ListValue(items...)).Build()),
				query.WithResponsePartLimitSizeBytes(partSize),
			)
			if err != nil {
				return err
			}

			rowCount := 0
			for {
				_, err = rs.NextRow(scope.Ctx)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				rowCount++
			}
			require.Equal(t, targetCount, rowCount)

			partCount = readPartCount - oldParts
			return nil
		})

		require.NoError(t, err)
		return partCount
	}

	partsWithBigSize := getPartCount(1000000)
	partsWithLittleSize := getPartCount(100)

	require.Equal(t, 1, partsWithBigSize)
	require.Greater(t, partsWithLittleSize, 1)
}
