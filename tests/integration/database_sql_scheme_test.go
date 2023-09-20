//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestDatabaseSqlScheme(t *testing.T) {
	var (
		scope = newScope(t)
		db    = scope.SQLDriverWithFolder()
	)

	defer func() {
		_ = db.Close()
	}()

	t.Run("drop-tables", func(t *testing.T) {
		cc, err := db.Conn(scope.Ctx)
		require.NoError(t, err)

		defer func() {
			_ = cc.Close()
		}()

		tables := make([]string, 0)
		err = cc.Raw(func(drvConn interface{}) error {
			q, ok := drvConn.(interface {
				GetTables(ctx context.Context, folder string, recursive bool, excludeSysDirs bool) ([]string, error)
			})
			if !ok {
				return fmt.Errorf("drvConn does not implement scheme methods")
			}

			tables, err = q.GetTables(scope.Ctx, ".", false, true)
			return err
		})
		require.NoError(t, err)

		for _, pathToTable := range tables {
			_, err = db.ExecContext(ydb.WithQueryMode(scope.Ctx, ydb.SchemeQueryMode),
				fmt.Sprintf("DROP TABLE `%s`", pathToTable))

			require.NoError(t, err)
		}
	})

	t.Run("create-tables", func(t *testing.T) {
		_, err := db.ExecContext(
			ydb.WithQueryMode(scope.Ctx, ydb.SchemeQueryMode),
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

		require.NoError(t, err)
	})

	t.Run("check-table-exists", func(t *testing.T) {
		cc, err := db.Conn(scope.Ctx)
		require.NoError(t, err)

		defer func() {
			_ = cc.Close()
		}()

		err = cc.Raw(func(drvConn interface{}) (err error) {
			q, ok := drvConn.(interface {
				IsTableExists(context.Context, string) (bool, error)
			})
			if !ok {
				return fmt.Errorf("drvConn does not implement scheme methods")
			}

			exists, err := q.IsTableExists(scope.Ctx, "series")
			if err != nil {
				return err
			}

			require.True(t, exists)

			if !exists {
				t.Logf("expected: table 'series' exists")
			}
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("get-columns", func(t *testing.T) {
		cc, err := db.Conn(scope.Ctx)
		require.NoError(t, err)

		defer func() {
			_ = cc.Close()
		}()

		err = cc.Raw(func(drvConn interface{}) (err error) {
			q, ok := drvConn.(interface {
				GetColumns(context.Context, string) ([]string, error)
			})
			if !ok {
				return fmt.Errorf("drvConn does not implement scheme methods")
			}

			columns, err := q.GetColumns(scope.Ctx, "series")
			if err != nil {
				return err
			}

			require.ElementsMatch(t,
				[]string{"series_id", "title", "series_info", "release_date", "comment"},
				columns)

			return nil
		})
		require.NoError(t, err)
	})
}
