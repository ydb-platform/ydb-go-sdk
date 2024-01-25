//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlIsPrimaryKey(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder()
	)

	defer func() {
		_ = db.Close()
	}()

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

			return err
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("is-primary-key", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			for _, pk := range []string{"series_id", "season_id", "episode_id"} {
				isPk := false
				err = cc.Raw(func(drvConn interface{}) (err error) {
					q, ok := drvConn.(interface {
						IsPrimaryKey(context.Context, string, string) (bool, error)
					})

					if !ok {
						return errors.New("drvConn does not implement extended API")
					}

					isPk, err = q.IsPrimaryKey(ctx, "episodes", pk)
					return err
				})

				if err != nil {
					return err
				}

				require.True(t, isPk)
			}
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("is-not-primary-key", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			for _, pk := range []string{"title", "air_date", "views"} {
				isPk := false
				err = cc.Raw(func(drvConn interface{}) (err error) {
					q, ok := drvConn.(interface {
						IsPrimaryKey(context.Context, string, string) (bool, error)
					})

					if !ok {
						return errors.New("drvConn does not implement extended API")
					}

					isPk, err = q.IsPrimaryKey(ctx, "episodes", pk)
					return err
				})

				if err != nil {
					return err
				}

				require.False(t, isPk)
			}
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}
