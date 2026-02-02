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

func TestDatabaseSqlDerivedTypesArgs(t *testing.T) {
	scope := newScope(t)
	db := scope.SQLDriverWithFolder(
		ydb.WithTablePathPrefix(scope.Folder()),
		ydb.WithAutoDeclare(),
		ydb.WithNumericArgs(),
	)

	var (
		inputInt32   = testInt32(42)
		inputUint64  = testUint64(100)
		inputFloat64 = testFloat64(3.14)
		inputString  = testString("hello")
		inputBool    = testBool(true)
		inputBytes   = testBytes("world")
		dt           = time.Date(2023, 3, 1, 16, 34, 18, 0, time.UTC)
	)

	var row *sql.Row
	err := retry.Retry(scope.Ctx, func(ctx context.Context) (err error) {
		row = db.QueryRowContext(ctx, `
			SELECT 
				$1 AS vInt32,
				$2 AS vUint64,
				$3 AS vFloat64,
				$4 AS vString,
				$5 AS vBool,
				$6 AS vBytes,
				$7 AS vDateTime 
		`, inputInt32, inputUint64, inputFloat64, inputString, inputBool, inputBytes, dt)
		return row.Err()
	})
	scope.Require.NoError(err)

	var (
		resInt32    testInt32
		resUint64   testUint64
		resFloat64  testFloat64
		resString   testString
		resBool     testBool
		resBytes    testBytes
		resDateTime time.Time
	)
	scope.Require.NoError(row.Scan(&resInt32, &resUint64, &resFloat64, &resString, &resBool, &resBytes, &resDateTime))

	scope.Require.Equal(inputInt32, resInt32)
	scope.Require.Equal(inputUint64, resUint64)
	scope.Require.Equal(inputFloat64, resFloat64)
	scope.Require.Equal(inputString, resString)
	scope.Require.Equal(inputBool, resBool)
	scope.Require.Equal(inputBytes, resBytes)
	scope.Require.Equal(dt, resDateTime.UTC())
}
