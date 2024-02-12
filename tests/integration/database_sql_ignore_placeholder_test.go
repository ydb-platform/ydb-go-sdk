//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlDiscardColumn(t *testing.T) {
	scope := newScope(t)
	db := scope.SQLDriverWithFolder(
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)
	dt := time.Date(2023, 3, 1, 16, 34, 18, 0, time.UTC)

	var row *sql.Row
	err := retry.Retry(scope.Ctx, func(ctx context.Context) (err error) {
		row = db.QueryRowContext(ctx, `
			SELECT 
				$1 AS vInt,
				$2 AS __discard_column_1,
				$3 AS __discard_column_2,
				$4 AS __discard_column_3 
			`, 1, "2", 3.0, dt,
		)
		return row.Err()
	})
	scope.Require.NoError(err)

	var resInt int
	scope.Require.NoError(row.Scan(&resInt))

	scope.Require.Equal(1, resInt)
}
