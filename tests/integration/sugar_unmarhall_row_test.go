//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestSugarUnmarshallRow(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	var (
		scope = newScope(t)
		db    = scope.Driver()
	)

	t.Run("HappyWay", func(t *testing.T) {
		type myStruct struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}

		row, err := db.Query().ReadRow(ctx, `SELECT 42 as id, "my string" as myStr`)
		require.NoError(t, err)

		one, err := sugar.UnmarshallRow[myStruct](row)
		require.NoError(t, err)
		require.NotNil(t, one)
		require.EqualValues(t, 42, one.ID)
		require.EqualValues(t, "my string", one.Str)
	})
	t.Run("UnexpectedColumn", func(t *testing.T) {
		type myStruct struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}

		row, err := db.Query().ReadRow(ctx, `SELECT 42 as id, "my string" as myStr, 123 as unexpected_column`)
		require.NoError(t, err)

		one, err := sugar.UnmarshallRow[myStruct](row)
		require.ErrorIs(t, err, scanner.ErrFieldsNotFoundInStruct)
		require.Nil(t, one)
	})
	t.Run("UnexpectedStructField", func(t *testing.T) {
		type myStruct struct {
			ID              int32  `sql:"id"`
			Str             string `sql:"myStr"`
			UnexpectedField int    `sql:"unexpected_column"`
		}

		row, err := db.Query().ReadRow(ctx, `SELECT 42 as id, "my string" as myStr`)
		require.NoError(t, err)

		one, err := sugar.UnmarshallRow[myStruct](row)
		require.ErrorIs(t, err, scanner.ErrColumnsNotFoundInRow)
		require.Nil(t, one)
	})
}
