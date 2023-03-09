//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestDatabaseSqlGetAllTables(t *testing.T) {
	var (
		scope  = newScope(t)
		db     = scope.SQLDriverWithFolder()
		folder = t.Name()
	)

	defer func() {
		_ = db.Close()
	}()

	t.Run("clear-folder", func(t *testing.T) {
		cc, err := ydb.Unwrap(db)
		require.NoError(t, err)

		err = sugar.RemoveRecursive(scope.Ctx, cc, folder)
		require.NoError(t, err)

		err = sugar.MakeRecursive(scope.Ctx, cc, folder)
		require.NoError(t, err)

		err = sugar.MakeRecursive(scope.Ctx, cc, path.Join(folder, "subdir"))
		require.NoError(t, err)
	})

	t.Run("create-tables", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				`CREATE TABLE episodes (
				series_id Uint64,
				season_id Uint64,
				episode_id Uint64,
				title UTF8,
				air_date Date,
				views Uint64,
				PRIMARY KEY (
					series_id,
					season_id,
					episode_id
				)
			)`,
			)

			if err != nil {
				return err
			}

			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				`
			CREATE TABLE series (
				series_id Uint64,
				title UTF8,
				series_info UTF8,
				release_date Date,
				comment UTF8,
				PRIMARY KEY (
					series_id
				)
			);`,
			)

			if err != nil {
				return err
			}

			_, err = cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				`CREATE TABLE `+"`./subdir/seasons`"+` (
				series_id Uint64,
				season_id Uint64,
				title UTF8,
				first_aired Date,
				last_aired Date,
				PRIMARY KEY (
					series_id,
					season_id
				)
			)`,
			)

			if err != nil {
				return err
			}

			return nil
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-current-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetAllTables(context.Context, string) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetAllTables(ctx, ".")
				return err
			})

			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"series", "episodes", "subdir/seasons"},
				tables)
			return nil
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-relative-sub-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetAllTables(context.Context, string) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetAllTables(ctx, "./subdir")
				return err
			})

			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-path-represent-a-table", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetAllTables(context.Context, string) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetAllTables(ctx, "./subdir/seasons")
				return err
			})

			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})
}
