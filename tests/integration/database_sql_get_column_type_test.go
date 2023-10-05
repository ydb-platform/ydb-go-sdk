//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlGetColumnType(t *testing.T) {
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
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})

	t.Run("get-column-type", func(t *testing.T) {
		err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
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
		}, retry.WithIdempotent(true))

		require.NoError(t, err)
	})
}

func TestDatabaseSqlColumnTypes(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder()
	)

	defer func() {
		_ = db.Close()
	}()

	columns := []struct {
		YQL      string
		Nullable bool
	}{
		{
			YQL:      "CAST(true AS Bool)",
			Nullable: false,
		},
		{
			YQL:      "CAST(true AS Optional<Bool>)",
			Nullable: true,
		},
		{
			YQL:      "CAST(123 AS Int32)",
			Nullable: false,
		},
		{
			YQL:      "CAST(456 AS Optional<Int32>)",
			Nullable: true,
		},
	}
	var result []*sql.ColumnType

	err := retry.Do(scope.Ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
		entries := make([]string, 0, len(columns))
		for i := range columns {
			entries = append(entries, columns[i].YQL)
		}

		rows, err := cc.QueryContext(ctx, fmt.Sprintf("SELECT %s", strings.Join(entries, ", ")))
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		result, err = rows.ColumnTypes()
		if err != nil {
			return err
		}

		return nil
	}, retry.WithIdempotent(true))

	require.NoError(t, err)

	for i := range columns {
		v, ok := result[i].Nullable()
		if assert.True(t, ok, "nullable not supported") {
			assert.Equal(t, columns[i].Nullable, v, "nullable didn't match")
		}
	}
}
