//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlPositionalArgs(t *testing.T) {
	scope := newScope(t)
	db := scope.SQLDriverWithFolder()
	dt := time.Date(2023, 3, 1, 16, 34, 18, 0, time.UTC)

	var row *sql.Row
	err := retry.Retry(scope.Ctx, func(ctx context.Context) (err error) {
		row = db.QueryRowContext(ctx, `
			SELECT 
				? AS vInt,
				? AS vText,
				? AS vDouble,
				? AS vDateTime 
			`, 1, "2", 3.0, dt,
		)
		return row.Err()
	})
	scope.Require.NoError(err)

	var resInt int
	var resText string
	var resDouble float64
	var resDateTime time.Time
	scope.Require.NoError(row.Scan(&resInt, &resText, &resDouble, &resDateTime))

	scope.Require.Equal(1, resInt)
	scope.Require.Equal("2", resText)
	scope.Require.Equal(3.0, resDouble)
	scope.Require.Equal(dt, resDateTime.UTC())
}
