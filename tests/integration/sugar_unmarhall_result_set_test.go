//go:build integration
// +build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestSugarUnmarshallResultSet(t *testing.T) {
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

		rows, err := db.Query().QueryResultSet(ctx, `
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
	t.Run("ListField", func(t *testing.T) {
		type myStruct struct {
			Ids []int32 `sql:"ids"`
		}

		rows, err := db.Query().QueryResultSet(ctx, `SELECT AsList(42, 43) as ids`)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.NoError(t, err)
		require.Len(t, many, 1)
		require.NotNil(t, many[0])
		require.Len(t, many[0].Ids, 2)
		require.EqualValues(t, 42, many[0].Ids[0])
		require.EqualValues(t, 43, many[0].Ids[1])
	})
	t.Run("SetField", func(t *testing.T) {
		type myStruct struct {
			Ids []int32 `sql:"ids"`
		}

		rows, err := db.Query().QueryResultSet(ctx, `SELECT AsSet(42, 43) as ids`)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.NoError(t, err)
		require.Len(t, many, 1)
		require.NotNil(t, many[0])
		require.Len(t, many[0].Ids, 2)
		require.EqualValues(t, 42, many[0].Ids[0])
		require.EqualValues(t, 43, many[0].Ids[1])
	})
	t.Run("StructField", func(t *testing.T) {
		type myStructField struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}
		type myStruct struct {
			ID          int32         `sql:"id"`
			Str         string        `sql:"myStr"`
			StructField myStructField `sql:"structColumn"`
		}

		rows, err := db.Query().QueryResultSet(ctx, `
			SELECT 42 as id, "myStr42" as myStr, AsStruct(22 as id, "myStr22" as myStr) as structColumn
		`)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.NoError(t, err)
		require.Len(t, many, 1)
		require.NotNil(t, many[0])
		require.EqualValues(t, 42, many[0].ID)
		require.EqualValues(t, "myStr42", many[0].Str)
		require.EqualValues(t, 22, many[0].StructField.ID)
		require.EqualValues(t, "myStr22", many[0].StructField.Str)
	})
	t.Run("ListOfStructsField", func(t *testing.T) {
		type myStructField struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}
		type myStruct struct {
			Values []myStructField `sql:"values"`
		}

		rows, err := db.Query().QueryResultSet(ctx,
			`SELECT AsList(AsStruct(22 as id, "myStr22" as myStr), AsStruct(42 as id, "myStr42" as myStr)) as values`,
		)

		many, err := sugar.UnmarshallResultSet[myStruct](rows)
		require.NoError(t, err)
		require.Len(t, many, 1)
		require.NotNil(t, many[0])
		require.Len(t, many[0].Values, 2)
		require.EqualValues(t, 22, many[0].Values[0].ID)
		require.EqualValues(t, "myStr22", many[0].Values[0].Str)
		require.EqualValues(t, 42, many[0].Values[1].ID)
		require.EqualValues(t, "myStr42", many[0].Values[1].Str)
	})
	t.Run("UnexpectedColumn", func(t *testing.T) {
		type myStruct struct {
			ID  int32  `sql:"id"`
			Str string `sql:"myStr"`
		}

		rows, err := db.Query().QueryResultSet(ctx, `
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

		rows, err := db.Query().QueryResultSet(ctx, `
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
