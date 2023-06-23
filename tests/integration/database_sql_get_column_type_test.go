//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlGetColumnType(t *testing.T) {
	scope := newScope(t)

	// create table
	err := retry.Do(scope.Ctx, scope.SQLDriverWithFolder(), func(ctx context.Context, cc *sql.Conn) (err error) {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE episodes (
				series_id Uint64 NOT NULL,
				season_id Uint64 NOT NULL,
				episode_id Uint64 NOT NULL,
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
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	require.NoError(t, err)

	// check columns type
	err = retry.Do(scope.Ctx, scope.SQLDriverWithFolder(), func(ctx context.Context, cc *sql.Conn) (err error) {
		columns := []struct {
			columnName string
			columnType string
		}{
			{
				columnName: "series_id",
				columnType: "UINT64",
			},
			{
				columnName: "season_id",
				columnType: "UINT64",
			},
			{
				columnName: "episode_id",
				columnType: "UINT64",
			},
			{
				columnName: "title",
				columnType: "OPTIONAL<UTF8>",
			},
			{
				columnName: "air_date",
				columnType: "OPTIONAL<DATE>",
			},
			{
				columnName: "views",
				columnType: "OPTIONAL<UINT64>",
			},
		}

		result := make([]struct {
			columnName string
			columnType string
		}, 0)

		for _, column := range []string{"series_id", "season_id", "episode_id", "title", "air_date", "views"} {
			err = cc.Raw(func(drvConn interface{}) (err error) {
				q, ok := drvConn.(interface {
					GetColumnType(context.Context, string, string) (string, error)
				})

				if !ok {
					return errors.New("drvConn does not implement extended API")
				}

				var columnType string
				columnType, err = q.GetColumnType(ctx, "episodes", column)
				result = append(result, struct {
					columnName string
					columnType string
				}{
					columnName: column,
					columnType: strings.ToUpper(columnType),
				})
				return err
			})

			if err != nil {
				return err
			}
		}

		require.ElementsMatch(t, columns, result)
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	require.NoError(t, err)
}
