//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsql/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestRegressionCloud109307(t *testing.T) {
	var (
		ctx   = xtest.Context(t)
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
		)
	)

	ctx, cancel := context.WithTimeout(ctx, 42*time.Second)
	defer cancel()

	for i := int64(1); ; i++ {
		if ctx.Err() != nil {
			break
		}

		err := retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			//nolint:gosec
			if rand.Int31n(3) == 0 {
				return badconn.Map(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)))
			}
			var rows *sql.Rows
			rows, err := tx.QueryContext(ctx, `SELECT $i`, sql.Named("i", i))
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
		}), retry.WithIdempotent(true))
		if ctx.Err() == nil {
			require.NoError(t, err)
		}
	}
}

func TestRegressionKikimr17104(t *testing.T) {
	var (
		ctx   = xtest.Context(t)
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder(
			ydb.WithTablePathPrefix(scope.Folder()),
			ydb.WithAutoDeclare(),
		)
		tableName       = t.Name()
		upsertRowsCount = 100000
		upsertChecksum  uint64
	)

	t.Run("data", func(t *testing.T) {
		t.Run("prepare", func(t *testing.T) {
			t.Run("scheme", func(t *testing.T) {
				err := retry.Do(ydb.WithQueryMode(ctx, ydb.SchemeQueryMode), db,
					func(ctx context.Context, cc *sql.Conn) (err error) {
						_, err = cc.ExecContext(ctx,
							fmt.Sprintf("CREATE TABLE %s (val Int32, PRIMARY KEY (val))", tableName),
						)
						if err != nil {
							return err
						}
						return nil
					}, retry.WithIdempotent(true),
				)
				require.NoError(t, err)
			})
			t.Run("data", func(t *testing.T) {
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
						_, err = cc.ExecContext(ctx,
							fmt.Sprintf("UPSERT INTO %s SELECT val FROM AS_TABLE($values);", tableName),
							table.NewQueryParameters(table.ValueParam("$values", types.ListValue(values...))),
						)
						if err != nil {
							return err
						}
						return nil
					}, retry.WithIdempotent(true),
				)
				require.NoError(t, err)
			})
		})
		t.Run("scan", func(t *testing.T) {
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
						rows, err = cc.QueryContext(ctx, fmt.Sprintf("SELECT val FROM %s", tableName))
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
					}, retry.WithIdempotent(true),
				)
				require.NoError(t, err)
				require.Equal(t, upsertRowsCount, rowsCount)
				require.Equal(t, upsertChecksum, checkSum)
			})
		})
	})
}
