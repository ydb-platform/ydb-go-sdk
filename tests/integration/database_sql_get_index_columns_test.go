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

func TestDatabaseSqlGetIndexColumns(t *testing.T) {
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

	t.Run("get-index-columns", func(t *testing.T) {
		for _, test := range []struct {
			IndexName      string
			IndexedColumns []string
		}{
			{
				IndexName:      "index_series_title",
				IndexedColumns: []string{"title"},
			},
			{
				IndexName:      "index_seasons_aired_date",
				IndexedColumns: []string{"first_aired", "last_aired"},
			},
		} {
			err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
				columns := make([]string, 0)
				err = cc.Raw(func(drvConn interface{}) (err error) {
					q, ok := drvConn.(interface {
						GetIndexColumns(context.Context, string, string) ([]string, error)
					})

					if !ok {
						return errors.New("drvConn does not implement extended API")
					}

					columns, err = q.GetIndexColumns(ctx, "./seasons", test.IndexName)
					return err
				})

				if err != nil {
					return err
				}

				require.ElementsMatch(t,
					test.IndexedColumns,
					columns)
				return nil
			}, retry.WithIdempotent(true))

			require.NoError(t, err)
		}
	})
}
