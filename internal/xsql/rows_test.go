package xsql

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowByAstPlan(t *testing.T) {
	t.Run("CreateRow", func(t *testing.T) {
		ast := "test ast"
		plan := "test plan"
		row := rowByAstPlan(ast, plan)

		require.NotNil(t, row)
		require.Len(t, row.values, 2)
		require.Equal(t, "Ast", row.values[0].Name)
		require.Equal(t, ast, row.values[0].Value)
		require.Equal(t, "Plan", row.values[1].Name)
		require.Equal(t, plan, row.values[1].Value)
		require.False(t, row.readAll)
	})
}

func TestSingleRow_Columns(t *testing.T) {
	row := rowByAstPlan("ast", "plan")
	columns := row.Columns()

	require.Len(t, columns, 2)
	require.Equal(t, "Ast", columns[0])
	require.Equal(t, "Plan", columns[1])
}

func TestSingleRow_Close(t *testing.T) {
	row := rowByAstPlan("ast", "plan")
	err := row.Close()
	require.NoError(t, err)
}

func TestSingleRow_Next(t *testing.T) {
	t.Run("FirstRead", func(t *testing.T) {
		ast := "test ast"
		plan := "test plan"
		row := rowByAstPlan(ast, plan)

		dst := make([]driver.Value, 2)
		err := row.Next(dst)

		require.NoError(t, err)
		require.Equal(t, ast, dst[0])
		require.Equal(t, plan, dst[1])
		require.True(t, row.readAll)
	})

	t.Run("SecondRead", func(t *testing.T) {
		row := rowByAstPlan("ast", "plan")

		dst := make([]driver.Value, 2)
		err := row.Next(dst)
		require.NoError(t, err)

		err = row.Next(dst)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("NilValues", func(t *testing.T) {
		row := &singleRow{
			values:  nil,
			readAll: false,
		}

		dst := make([]driver.Value, 2)
		err := row.Next(dst)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("EmptyValues", func(t *testing.T) {
		row := &singleRow{
			values:  []sql.NamedArg{},
			readAll: false,
		}

		dst := make([]driver.Value, 0)
		err := row.Next(dst)
		require.NoError(t, err)
		require.True(t, row.readAll)
	})
}
