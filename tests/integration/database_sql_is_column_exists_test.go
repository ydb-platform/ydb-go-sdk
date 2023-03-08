//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestDatabaseSqlIsColumnExists(t *testing.T) {
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
	})

	t.Run("create-tables", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
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

			return err
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})

	t.Run("is-column-exists", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			for _, column := range []string{"series_id", "title", "series_info", "release_date", "comment"} {
				var exists bool = true
				err = cc.Raw(func(drvConn interface{}) (err error) {
					q, ok := drvConn.(interface {
						IsColumnExists(context.Context, string, string) (bool, error)
					})

					if !ok {
						return errors.New("drvConn does not implement extended API")
					}

					exists, err = q.IsColumnExists(ctx, "series", column)
					return err
				})

				if err != nil {
					return err
				}

				require.True(t, exists)
			}
			return nil
		}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))

		require.NoError(t, err)
	})
}
