//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestDatabaseSqlGetTables(t *testing.T) {
	var (
		scope  = newScope(t)
		db     = scope.SQLDriverWithFolder()
		folder = t.Name()
	)

	defer func() {
		_ = db.Close()
	}()

	t.Run("prepare-sub-folder", func(t *testing.T) {
		cc, err := ydb.Unwrap(db)
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
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-tables-at-current-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, ".", false, true)
				return err
			})

			if err != nil {
				return err
			}

			require.Equal(t,
				[]string{"episodes", "series"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-tables-at-relative-sub-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, "./subdir", false, true)
				return err
			})

			if err != nil {
				return err
			}

			require.Equal(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-tables-at-path-represent-a-table", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, "./subdir/seasons", false, true)
				return err
			})

			if err != nil {
				return err
			}

			require.Equal(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}

func TestDatabaseSqlGetTablesRecursive(t *testing.T) {
	var (
		scope  = newScope(t)
		db     = scope.SQLDriverWithFolder()
		folder = t.Name()
	)

	defer func() {
		_ = db.Close()
	}()

	t.Run("prepare-sub-folder", func(t *testing.T) {
		cc, err := ydb.Unwrap(db)
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
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-current-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, ".", true, true)
				return err
			})

			if err != nil {
				return err
			}

			sort.Strings(tables)

			require.Equal(t,
				[]string{"episodes", "series", "subdir/seasons"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-relative-sub-folder", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, "./subdir", true, true)
				return err
			})

			if err != nil {
				return err
			}

			sort.Strings(tables)

			require.Equal(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-all-tables-at-path-represent-a-table", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			tables := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				tables, err = q.GetTables(ctx, "./subdir/seasons", true, true)
				return err
			})

			if err != nil {
				return err
			}

			sort.Strings(tables)

			require.Equal(t,
				[]string{"seasons"},
				tables)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}
