//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestDatabaseSqlYSONColumn(t *testing.T) {
	engines := []struct {
		name         string
		queryService bool
	}{
		{"QueryService", true},
		{"TableService", false},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			scope := newScope(t)
			ctx := scope.Ctx
			tablePath := scope.TablePath(withCreateTableQueryTemplate(`
				PRAGMA TablePathPrefix("{{.TablePathPrefix}}");
				CREATE TABLE {{.TableName}} (
					id Int64 NOT NULL,
					y Yson,
					PRIMARY KEY (id)
				)
			`))

			db := scope.SQLDriverWithFolder(ydb.WithQueryService(engine.queryService))
			defer db.Close()

			textYSON := "<a=1>[3;%false]"
			bytesYSON := []byte("<a=2>[4;%true]")

			_, err := db.ExecContext(ctx, fmt.Sprintf("DECLARE $ysonText AS Yson; UPSERT INTO `%s` (id, y) VALUES (1, $ysonText);", tablePath), sql.Named("ysonText", textYSON))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, fmt.Sprintf("DECLARE $ysonBytes AS Yson; UPSERT INTO `%s` (id, y) VALUES (2, $ysonBytes);", tablePath), sql.Named("ysonBytes", bytesYSON))
			require.NoError(t, err)

			t.Run("scan to string", func(t *testing.T) {
				row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT y FROM `%s` WHERE id = 1;", tablePath))
				var gotTextAsString string
				require.NoError(t, row.Scan(&gotTextAsString))
				require.Equal(t, textYSON, gotTextAsString)

				row = db.QueryRowContext(ctx, fmt.Sprintf("SELECT y FROM `%s` WHERE id = 2;", tablePath))
				var gotBytesAsString string
				require.NoError(t, row.Scan(&gotBytesAsString))
				require.Equal(t, string(bytesYSON), gotBytesAsString)
			})

			t.Run("scan to []byte", func(t *testing.T) {
				row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT y FROM `%s` WHERE id = 1;", tablePath))
				var gotTextAsBytes []byte
				require.NoError(t, row.Scan(&gotTextAsBytes))
				require.Equal(t, []byte(textYSON), gotTextAsBytes)

				row = db.QueryRowContext(ctx, fmt.Sprintf("SELECT y FROM `%s` WHERE id = 2;", tablePath))
				var gotBytesAsBytes []byte
				require.NoError(t, row.Scan(&gotBytesAsBytes))
				require.Equal(t, bytesYSON, gotBytesAsBytes)
			})
		})
	}
}
