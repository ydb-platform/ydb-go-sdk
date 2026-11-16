package xquery

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestRowsColumnTypesMappedToVisibleColumns(t *testing.T) {
	optionalInt32 := types.NewOptional(types.Int32)
	r := &rows{
		next: &resultSet{
			columns:      []string{"visible_1", "visible_2"},
			columnsTypes: []types.Type{types.Int32, types.Text, optionalInt32},
			visibleTypes: []int{0, 2},
		},
	}

	require.Equal(t, optionalInt32.Yql(), r.ColumnTypeDatabaseTypeName(t.Context(), 1))

	nullable, ok := r.ColumnTypeNullable(t.Context(), 1)
	require.True(t, ok)
	require.True(t, nullable)

	nullable, ok = r.ColumnTypeNullable(t.Context(), 0)
	require.True(t, ok)
	require.False(t, nullable)
}
