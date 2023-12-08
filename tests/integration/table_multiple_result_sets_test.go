//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type scopeTableStreamExecuteScanQuery struct {
	folder          string
	tableName       string
	upsertRowsCount int

	sum uint64
}

func TestTableMultipleResultSets(t *testing.T) {
	var (
		scope = &scopeTableStreamExecuteScanQuery{
			folder:          t.Name(),
			tableName:       "stream_query_table",
			upsertRowsCount: 100000,
			sum:             0,
		}
		ctx = xtest.Context(t)
	)

	db, err := ydb.Open(ctx,
		"", // corner case for check replacement of endpoint+database+secure
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithLogger(
			newLogger(t),
			trace.MatchDetails(`ydb\.(driver|discovery|retry|scheme).*`),
		),
	)
	require.NoError(t, err)

	defer func() {
		err = db.Close(ctx)
		require.NoError(t, err)
	}()

	t.Run("create", func(t *testing.T) {
		t.Run("table", func(t *testing.T) {
			err = db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					_ = s.ExecuteSchemeQuery(
						ctx, `
						PRAGMA TablePathPrefix("`+path.Join(db.Name(), scope.folder)+`");
						DROP TABLE `+scope.tableName+`;`,
					)
					return s.ExecuteSchemeQuery(
						ctx, `
						PRAGMA TablePathPrefix("`+path.Join(db.Name(), scope.folder)+`");
						CREATE TABLE `+scope.tableName+` (val Int32, PRIMARY KEY (val));`,
					)
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("upsert", func(t *testing.T) {
		t.Run("data", func(t *testing.T) {
			// - upsert data
			values := make([]types.Value, 0, scope.upsertRowsCount)
			for i := 0; i < scope.upsertRowsCount; i++ {
				scope.sum += uint64(i)
				values = append(
					values,
					types.StructValue(
						types.StructFieldValue("val", types.Int32Value(int32(i))),
					),
				)
			}
			err := db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					_, _, err = s.Execute(ctx,
						table.TxControl(
							table.BeginTx(
								table.WithSerializableReadWrite(),
							),
							table.CommitTx(),
						), `
							PRAGMA TablePathPrefix("`+path.Join(db.Name(), scope.folder)+`");
							DECLARE $values AS List<Struct<
								val: Int32,
							> >;
							UPSERT INTO `+scope.tableName+`
							SELECT
								val 
							FROM
								AS_TABLE($values);            
						`, table.NewQueryParameters(
							table.ValueParam(
								"$values",
								types.ListValue(values...),
							),
						),
					)
					return err
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("scan", func(t *testing.T) {
		t.Run("scan", func(t *testing.T) {
			err := db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					res, err := s.StreamExecuteScanQuery(
						ctx, `
							PRAGMA TablePathPrefix("`+path.Join(db.Name(), scope.folder)+`");
							SELECT val FROM `+scope.tableName+`;`,
						table.NewQueryParameters(),
						options.WithExecuteScanQueryStats(options.ExecuteScanQueryStatsTypeFull),
					)
					if err != nil {
						return err
					}
					var (
						resultSetsCount = 0
						rowsCount       = 0
						checkSum        uint64
					)
					for res.NextResultSet(ctx) {
						resultSetsCount++
						for res.NextRow() {
							rowsCount++
							var val *int32
							err = res.Scan(&val)
							if err != nil {
								return err
							}
							checkSum += uint64(*val)
						}
						if stats := res.Stats(); stats != nil {
							t.Logf(" --- query stats: compilation: %v, process CPU time: %v, affected shards: %v\n",
								stats.Compilation(),
								stats.ProcessCPUTime(),
								func() (count uint64) {
									for {
										phase, ok := stats.NextPhase()
										if !ok {
											return
										}
										count += phase.AffectedShards()
									}
								}(),
							)
						}
					}

					if err = res.Err(); err != nil {
						return err
					}

					if rowsCount != scope.upsertRowsCount {
						return fmt.Errorf("wrong rows count: %v, exp: %v", rowsCount, scope.upsertRowsCount)
					}

					if scope.sum != checkSum {
						return fmt.Errorf("wrong checkSum: %v, exp: %v", checkSum, scope.sum)
					}

					if resultSetsCount <= 1 {
						return fmt.Errorf("wrong result sets count: %v", resultSetsCount)
					}

					return nil
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})
}
