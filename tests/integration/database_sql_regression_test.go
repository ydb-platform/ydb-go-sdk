//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/conn/badconn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
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

func TestUUIDSerializationDatabaseSQLIssue1501(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/1501
	// test with special uuid - all bytes are different for check any byte swaps

	t.Run("old-send", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := [16]byte(uuid.MustParse(idString))
		row := db.QueryRow(`
DECLARE $val AS UUID;

SELECT CAST($val AS Utf8)`, sql.Named("val", id),
		)

		require.ErrorIs(t, row.Err(), types.ErrIssue1501BadUUID)
	})
	t.Run("old-send-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "2d9e498b-b746-9cfb-084d-de4e1cb4736e"
		id := [16]byte(uuid.MustParse(idString))
		row := db.QueryRow(`
DECLARE $val AS UUID;

SELECT CAST($val AS Utf8)`,
			sql.Named("val", types.NewUUIDBytesWithIssue1501(id)),
		)

		require.NoError(t, row.Err())

		var res string

		err := row.Scan(&res)
		require.NoError(t, err)
		require.Equal(t, expectedResultWithBug, res)
	})
	t.Run("old-receive-to-bytes", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		row := db.QueryRow(`
DECLARE $val AS Text;

SELECT CAST($val AS UUID)`,
			sql.Named("val", idString),
		)

		require.NoError(t, row.Err())

		var res [16]byte

		err := row.Scan(&res)
		require.Error(t, err)
	})
	t.Run("old-receive-to-bytes-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		expectedResultWithBug := "8b499e2d-46b7-fb9c-4d08-4ede6e73b41c"
		row := db.QueryRow(`
DECLARE $val AS Text;

SELECT CAST($val AS UUID)`,
			sql.Named("val", idString),
		)

		require.NoError(t, row.Err())

		var res types.UUIDBytesWithIssue1501Type

		err := row.Scan(&res)
		require.NoError(t, err)

		resUUID := uuid.UUID(res.AsBytesArray())
		require.Equal(t, expectedResultWithBug, resUUID.String())
	})

	t.Run("old-receive-to-string", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		row := db.QueryRow(`
DECLARE $val AS Text;

SELECT CAST($val AS UUID)`,
			sql.Named("val", idString),
		)

		require.NoError(t, row.Err())

		var res string

		err := row.Scan(&res)
		require.Error(t, err)
	})
	t.Run("old-receive-to-uuid", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		row := db.QueryRow(`
DECLARE $val AS Text;

SELECT CAST($val AS UUID)`,
			sql.Named("val", idString),
		)

		require.NoError(t, row.Err())

		var res uuid.UUID

		err := row.Scan(&res)
		require.Error(t, err)
	})
	t.Run("old-send-receive", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		idParam := [16]byte(id)
		row := db.QueryRow(`
DECLARE $val AS UUID;

SELECT $val`,
			sql.Named("val", idParam),
		)

		require.ErrorIs(t, row.Err(), types.ErrIssue1501BadUUID)
	})
	t.Run("old-send-receive-with-force-wrapper", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		row := db.QueryRow(`
DECLARE $val AS UUID;

SELECT $val`,
			sql.Named("val", types.UUIDWithIssue1501Value(id)),
		)

		require.NoError(t, row.Err())

		var resBytes types.UUIDBytesWithIssue1501Type
		err := row.Scan(&resBytes)
		require.NoError(t, err)

		resUUID := uuid.UUID(resBytes.AsBytesArray())

		require.Equal(t, id, resUUID)
	})
	t.Run("good-send", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		row := db.QueryRow(`
DECLARE $val AS Utf8;

SELECT $val`,
			sql.Named("val", id), // send as string because uuid implements Value() (driver.Value, error)
		)

		require.NoError(t, row.Err())

		var res string
		err := row.Scan(&res)
		require.NoError(t, err)
		require.Equal(t, id.String(), res)
	})
	t.Run("good-receive", func(t *testing.T) {
		// test old behavior - for test way of safe work with data, written with bagged API version
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		row := db.QueryRow(`
DECLARE $val AS Utf8;

SELECT CAST($val AS UUID)`,
			sql.Named("val", idString),
		)

		require.NoError(t, row.Err())

		var resFromDB types.UUIDBytesWithIssue1501Type

		err := row.Scan(&resFromDB)
		require.NoError(t, err)

		resUUID := resFromDB.PublicRevertReorderForIssue1501()

		resString := strings.ToUpper(resUUID.String())
		require.Equal(t, idString, resString)
	})
	t.Run("good-send-receive", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver()
		)

		idString := "6E73B41C-4EDE-4D08-9CFB-B7462D9E498B"
		id := uuid.MustParse(idString)
		row := db.QueryRow(`
DECLARE $val AS Utf8;

SELECT $val`,
			sql.Named("val", id),
		)

		require.NoError(t, row.Err())

		var res uuid.UUID
		err := row.Scan(&res)
		require.NoError(t, err)

		require.Equal(t, id.String(), res.String())
	})
}
