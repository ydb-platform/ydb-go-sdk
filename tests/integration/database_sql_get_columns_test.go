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

func TestDatabaseSqlGetColumns(t *testing.T) {
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

	t.Run("get-columns", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			columns := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetColumns(context.Context, string) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				columns, err = q.GetColumns(ctx, "episodes")
				return err
			})

			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"series_id", "season_id", "episode_id", "title", "air_date", "views"},
				columns)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}
