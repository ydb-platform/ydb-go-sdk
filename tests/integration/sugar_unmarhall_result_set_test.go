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

func TestSugarUnmarshallResultSet(t *testing.T) {
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

		rows, err := db.Query().ReadResultSet(ctx, `
			SELECT 42 as id, "myStr42" as myStr
			UNION
			SELECT 43 as id, "myStr43" as myStr
			ORDER BY id
		`)
		require.NoError(t, err)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.NoError(t, err)
		require.Len(t, many, 2)
		require.NotNil(t, many[0])
		require.NotNil(t, many[1])
		require.EqualValues(t, 42, many[0].ID)
		require.EqualValues(t, "myStr42", many[0].Str)
		require.EqualValues(t, 43, many[1].ID)
		require.EqualValues(t, "myStr43", many[1].Str)
	})
	t.Run("UnexpectedColumn", func(t *testing.T) {
		type myStruct struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}

		rows, err := db.Query().ReadResultSet(ctx, `
			SELECT 42 as id, "myStr42" as myStr, 123 as unexpected_column
			UNION
			SELECT 43 as id, "myStr43" as myStr, 123 as unexpected_column
			ORDER BY id
		`)
		require.NoError(t, err)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.ErrorIs(t, err, scanner.ErrFieldsNotFoundInStruct)
		require.Empty(t, many)
	})
	t.Run("UnexpectedStructField", func(t *testing.T) {
		type myStruct struct {
			ID              int32  `sql:"id"`
			Str             string `sql:"myStr"`
			UnexpectedField int    `sql:"unexpected_column"`
		}

		rows, err := db.Query().ReadResultSet(ctx, `
			SELECT 42 as id, "myStr42" as myStr
			UNION
			SELECT 43 as id, "myStr43" as myStr
			ORDER BY id
		`)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.ErrorIs(t, err, scanner.ErrColumnsNotFoundInRow)
		require.Empty(t, many)
	})
}
