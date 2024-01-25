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

func TestDatabaseSqlGetIndexes(t *testing.T) {
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
				`CREATE TABLE seasons (
				series_id Uint64,
				season_id Uint64,
				title UTF8,
				first_aired Date,
				last_aired Date,
				INDEX index_series_title GLOBAL ON (title),
				INDEX index_seasons_aired_date GLOBAL ON (first_aired, last_aired),
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

	t.Run("get-indexes", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
			indexes := make([]string, 0)
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetIndexes(context.Context, string) ([]string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				indexes, err = q.GetIndexes(ctx, "./seasons")
				return err
			})

			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"index_series_title", "index_seasons_aired_date"},
				indexes)
			return nil
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}
