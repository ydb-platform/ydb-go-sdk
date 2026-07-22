package scanner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

func TestValueScanner_ColumnCount(t *testing.T) {
	t.Run("nil result set", func(t *testing.T) {
		s := &valueScanner{}
		require.Equal(t, 0, s.ColumnCount())
	})

	t.Run("empty columns", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{},
			}.Build(),
		}
		require.Equal(t, 0, s.ColumnCount())
	})

	t.Run("with columns", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{Name: "col1"}.Build(),
					Ydb.Column_builder{Name: "col2"}.Build(),
					Ydb.Column_builder{Name: "col3"}.Build(),
				},
			}.Build(),
		}
		require.Equal(t, 3, s.ColumnCount())
	})
}

func TestValueScanner_Columns(t *testing.T) {
	t.Run("nil result set", func(t *testing.T) {
		s := &valueScanner{}
		var count int
		s.Columns(func(col options.Column) {
			count++
		})
		require.Equal(t, 0, count)
	})

	t.Run("iterate over columns", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "id",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_INT32.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "name",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
			}.Build(),
		}

		var names []string
		s.Columns(func(col options.Column) {
			names = append(names, col.Name)
		})
		require.Equal(t, []string{"id", "name"}, names)
	})
}

func TestValueScanner_RowCount(t *testing.T) {
	t.Run("nil result set", func(t *testing.T) {
		s := &valueScanner{}
		require.Equal(t, 0, s.RowCount())
	})

	t.Run("empty rows", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{},
			}.Build(),
		}
		require.Equal(t, 0, s.RowCount())
	})

	t.Run("with rows", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					{},
					{},
					{},
				},
			}.Build(),
		}
		require.Equal(t, 3, s.RowCount())
	})
}

func TestValueScanner_ItemCount(t *testing.T) {
	t.Run("nil row", func(t *testing.T) {
		s := &valueScanner{}
		require.Equal(t, 0, s.ItemCount())
	})

	t.Run("empty items", func(t *testing.T) {
		s := &valueScanner{
			row: Ydb.Value_builder{
				Items: []*Ydb.Value{},
			}.Build(),
		}
		require.Equal(t, 0, s.ItemCount())
	})

	t.Run("with items", func(t *testing.T) {
		s := &valueScanner{
			row: Ydb.Value_builder{
				Items: []*Ydb.Value{
					{},
					{},
				},
			}.Build(),
		}
		require.Equal(t, 2, s.ItemCount())
	})
}

func TestValueScanner_Truncated(t *testing.T) {
	t.Run("not truncated", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Truncated: false,
			}.Build(),
		}
		require.False(t, s.Truncated())
	})

	t.Run("truncated", func(t *testing.T) {
		s := &valueScanner{
			set: Ydb.ResultSet_builder{
				Truncated: true,
			}.Build(),
		}
		require.True(t, s.Truncated())
	})

	t.Run("nil set", func(t *testing.T) {
		s := &valueScanner{}
		require.False(t, s.Truncated())
	})
}
