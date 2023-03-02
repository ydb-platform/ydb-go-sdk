//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestRegressionCloud109307(t *testing.T) {
	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(xtest.Context(t), 42*time.Second)
	defer cancel()

	for i := int64(1); ; i++ {
		if ctx.Err() != nil {
			break
		}

		err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			//nolint:gosec
			if rand.Int31n(3) == 0 {
				return badconn.Map(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)))
			}
			var rows *sql.Rows
			rows, err = tx.QueryContext(ctx, `
				DECLARE $i AS Int64;

				SELECT $i;
			`, sql.Named("i", i))
			if err != nil {
				return err
			}
			defer rows.Close()
			if !rows.Next() {
				return errors.New("no rows")
			}
			var result interface{}
			if err = rows.Scan(&result); err != nil {
				return err
			}
			if result.(int64)%100 == 0 {
				t.Logf("result: %+v\n", result)
			}
			return rows.Err()
		}, retry.WithTxOptions(&sql.TxOptions{
			Isolation: sql.LevelSnapshot,
			ReadOnly:  true,
		}), retry.WithDoTxRetryOptions(
			retry.WithIdempotent(true),
		))
		if ctx.Err() == nil {
			require.NoError(t, err)
		}
	}
}

func TestRegressionKikimr17104(t *testing.T) {
	var (
		ctx               = xtest.Context(t)
		tableRelativePath = path.Join(t.Name(), "big_table")
		upsertRowsCount   = 100000
		upsertChecksum    uint64
	)

	t.Run("data", func(t *testing.T) {
		t.Run("prepare", func(t *testing.T) {
			var (
				db                *sql.DB
				tableAbsolutePath string
			)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			t.Run("connect", func(t *testing.T) {
				cc, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
				require.NoError(t, err)
				connector, err := ydb.Connector(cc)
				require.NoError(t, err)
				db = sql.OpenDB(connector)
				tableAbsolutePath = path.Join(cc.Name(), tableRelativePath)
			})
			t.Run("scheme", func(t *testing.T) {
				var cc *ydb.Driver
				t.Run("unwrap", func(t *testing.T) {
					var err error
					cc, err = ydb.Unwrap(db)
					require.NoError(t, err)
				})
				var tableExists bool
				t.Run("check_exists", func(t *testing.T) {
					var err error
					tableExists, err = sugar.IsTableExists(ctx, cc.Scheme(), tableAbsolutePath)
					require.NoError(t, err)
				})
				if tableExists {
					t.Run("drop", func(t *testing.T) {
						err := retry.Do(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), db,
							func(ctx context.Context, cc *sql.Conn) (err error) {
								_, err = cc.ExecContext(ctx,
									fmt.Sprintf("DROP TABLE `%s`", tableAbsolutePath),
								)
								if err != nil {
									return err
								}
								return nil
							}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
						)
						require.NoError(t, err)
					})
				}
				t.Run("create", func(t *testing.T) {
					err := retry.Do(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), db,
						func(ctx context.Context, cc *sql.Conn) (err error) {
							_, err = cc.ExecContext(ctx,
								fmt.Sprintf("CREATE TABLE `%s` (val Int32, PRIMARY KEY (val))", tableAbsolutePath),
							)
							if err != nil {
								return err
							}
							return nil
						}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
					)
					require.NoError(t, err)
				})
			})
			t.Run("upsert", func(t *testing.T) {
				if v, ok := os.LookupEnv("UPSERT_ROWS_COUNT"); ok {
					var vv int
					vv, err := strconv.Atoi(v)
					require.NoError(t, err)
					upsertRowsCount = vv
				}
				// - upsert data
				t.Logf("> preparing values to upsert...\n")
				values := make([]types.Value, 0, upsertRowsCount)
				for i := 0; i < upsertRowsCount; i++ {
					upsertChecksum += uint64(i)
					values = append(values,
						types.StructValue(
							types.StructFieldValue("val", types.Int32Value(int32(i))),
						),
					)
				}
				t.Logf("> upsert data\n")
				err := retry.Do(ydb.WithQueryMode(ctx, ydb.DataQueryMode), db,
					func(ctx context.Context, cc *sql.Conn) (err error) {
						values := table.NewQueryParameters(table.ValueParam("$values", types.ListValue(values...)))
						declares, err := sugar.GenerateDeclareSection(values)
						require.NoError(t, err)
						_, err = cc.ExecContext(ctx,
							declares+fmt.Sprintf("UPSERT INTO `%s` SELECT val FROM AS_TABLE($values);", tableAbsolutePath),
							values,
						)
						if err != nil {
							return err
						}
						return nil
					}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
				)
				require.NoError(t, err)
			})
		})
		t.Run("scan", func(t *testing.T) {
			var (
				db                *sql.DB
				tableAbsolutePath string
			)
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			t.Run("connect", func(t *testing.T) {
				cc, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
				require.NoError(t, err)
				connector, err := ydb.Connector(cc, ydb.WithDefaultQueryMode(ydb.ScanQueryMode))
				require.NoError(t, err)
				db = sql.OpenDB(connector)
				tableAbsolutePath = path.Join(cc.Name(), tableRelativePath)
			})
			t.Run("query", func(t *testing.T) {
				var (
					rowsCount int
					checkSum  uint64
				)
				err := retry.Do(ydb.WithQueryMode(ctx, ydb.ScanQueryMode), db,
					func(ctx context.Context, cc *sql.Conn) (err error) {
						var rows *sql.Rows
						rowsCount = 0
						checkSum = 0
						rows, err = cc.QueryContext(ctx, fmt.Sprintf("SELECT val FROM `%s`", tableAbsolutePath))
						if err != nil {
							return err
						}
						for rows.NextResultSet() {
							for rows.Next() {
								rowsCount++
								var val uint64
								err = rows.Scan(&val)
								if err != nil {
									return err
								}
								checkSum += val
							}
						}
						return rows.Err()
					}, retry.WithDoRetryOptions(retry.WithIdempotent(true)),
				)
				require.NoError(t, err)
				require.Equal(t, upsertRowsCount, rowsCount)
				require.Equal(t, upsertChecksum, checkSum)
			})
		})
	})
}
