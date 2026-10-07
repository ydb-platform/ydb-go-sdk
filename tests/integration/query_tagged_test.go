//go:build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestQueryTaggedResults(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()

	for _, tt := range []struct {
		name       string
		expression string
		typeYql    string
		valueYql   string
	}{
		{"scalar", `AsTagged(42, "tag")`, `Tagged<Int32,"tag">`, `AsTagged(42,"tag")`},
		{
			"nested",
			`AsTagged(AsTagged(42, "inner"), "outer")`,
			`Tagged<Tagged<Int32,"inner">,"outer">`,
			`AsTagged(AsTagged(42,"inner"),"outer")`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := db.Query().QueryRow(scope.Ctx, "SELECT "+tt.expression+" AS value;")
			require.NoError(t, err)

			var n int32
			require.NoError(t, row.Scan(&n))
			require.EqualValues(t, 42, n)
			n = 0
			require.NoError(t, row.ScanNamed(query.Named("value", &n)))
			require.EqualValues(t, 42, n)
			var scalar struct {
				Value int32 `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&scalar))
			require.EqualValues(t, 42, scalar.Value)
			var invalid bool
			require.Error(t, row.Scan(&invalid))

			values := row.Values()
			require.Len(t, values, 1)
			require.Equal(t, tt.typeYql, values[0].Type().Yql())
			require.Equal(t, tt.valueYql, values[0].Yql())

			var scanned types.Value
			require.NoError(t, row.Scan(&scanned))
			require.Equal(t, values[0], scanned)
			var named types.Value
			require.NoError(t, row.ScanNamed(query.Named("value", &named)))
			require.Equal(t, values[0], named)
			var dst struct {
				Value types.Value `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&dst))
			require.Equal(t, values[0], dst.Value)
		})
	}

	for _, tt := range []struct {
		name       string
		expression string
		typeYql    string
		valueYql   string
	}{
		{"optional", `Just(AsTagged(42, "tag"))`, `Optional<Tagged<Int32,"tag">>`, `Just(AsTagged(42,"tag"))`},
		{"tagged_optional", `AsTagged(Just(42), "tag")`, `Tagged<Optional<Int32>,"tag">`, `AsTagged(Just(42),"tag")`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := db.Query().QueryRow(scope.Ctx, "SELECT "+tt.expression+" AS value;")
			require.NoError(t, err)

			var n *int32
			require.NoError(t, row.Scan(&n))
			require.NotNil(t, n)
			require.EqualValues(t, 42, *n)
			var namedNumber *int32
			require.NoError(t, row.ScanNamed(query.Named("value", &namedNumber)))
			require.Equal(t, n, namedNumber)
			var number struct {
				Value *int32 `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&number))
			require.Equal(t, n, number.Value)

			values := row.Values()
			require.Len(t, values, 1)
			require.Equal(t, tt.typeYql, values[0].Type().Yql())
			require.Equal(t, tt.valueYql, values[0].Yql())

			var scanned types.Value
			require.NoError(t, row.Scan(&scanned))
			require.Equal(t, values[0], scanned)
			var named types.Value
			require.NoError(t, row.ScanNamed(query.Named("value", &named)))
			require.Equal(t, values[0], named)
			var dst struct {
				Value types.Value `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&dst))
			require.Equal(t, values[0], dst.Value)
		})
	}

	for _, tt := range []struct {
		name       string
		expression string
		typeYql    string
		valueYql   string
	}{
		{
			"null",
			`AsTagged(Nothing(Int32?), "tag")`,
			`Tagged<Optional<Int32>,"tag">`,
			`AsTagged(Nothing(Optional<Int32>),"tag")`,
		},
		{
			"optional_null",
			`Nothing(OptionalType(TypeOf(AsTagged(42, "tag"))))`,
			`Optional<Tagged<Int32,"tag">>`,
			`Nothing(Optional<Tagged<Int32,"tag">>)`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			row, err := db.Query().QueryRow(scope.Ctx, "SELECT "+tt.expression+" AS value;")
			require.NoError(t, err)

			var n *int32
			require.NoError(t, row.Scan(&n))
			require.Nil(t, n)
			var namedNumber *int32
			require.NoError(t, row.ScanNamed(query.Named("value", &namedNumber)))
			require.Nil(t, namedNumber)
			var number struct {
				Value *int32 `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&number))
			require.Nil(t, number.Value)

			values := row.Values()
			require.Len(t, values, 1)
			require.Equal(t, tt.typeYql, values[0].Type().Yql())
			require.Equal(t, tt.valueYql, values[0].Yql())

			var scanned types.Value
			require.NoError(t, row.Scan(&scanned))
			require.Equal(t, values[0], scanned)
			var named types.Value
			require.NoError(t, row.ScanNamed(query.Named("value", &named)))
			require.Equal(t, values[0], named)
			var dst struct {
				Value types.Value `sql:"value"`
			}
			require.NoError(t, row.ScanStruct(&dst))
			require.Equal(t, values[0], dst.Value)
		})
	}

	t.Run("nested_null", func(t *testing.T) {
		row, err := db.Query().QueryRow(scope.Ctx, `SELECT Just(AsTagged(Nothing(Int32?), "tag")) AS value;`)
		require.NoError(t, err)

		var n **int32
		require.NoError(t, row.Scan(&n))
		require.NotNil(t, n)
		require.Nil(t, *n)
		var namedNumber **int32
		require.NoError(t, row.ScanNamed(query.Named("value", &namedNumber)))
		require.Equal(t, n, namedNumber)
		var number struct {
			Value **int32 `sql:"value"`
		}
		require.NoError(t, row.ScanStruct(&number))
		require.Equal(t, n, number.Value)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, `Optional<Tagged<Optional<Int32>,"tag">>`, values[0].Type().Yql())
		require.Equal(t, `Just(AsTagged(Nothing(Optional<Int32>),"tag"))`, values[0].Yql())

		var scanned types.Value
		require.NoError(t, row.Scan(&scanned))
		require.Equal(t, values[0], scanned)
		var named types.Value
		require.NoError(t, row.ScanNamed(query.Named("value", &named)))
		require.Equal(t, values[0], named)
		var dst struct {
			Value types.Value `sql:"value"`
		}
		require.NoError(t, row.ScanStruct(&dst))
		require.Equal(t, values[0], dst.Value)
	})

	t.Run("list", func(t *testing.T) {
		row, err := db.Query().QueryRow(scope.Ctx, `SELECT [AsTagged(42, "tag")] AS value;`)
		require.NoError(t, err)

		var numbers []int32
		require.NoError(t, row.Scan(&numbers))
		require.Equal(t, []int32{42}, numbers)
		var namedNumbers []int32
		require.NoError(t, row.ScanNamed(query.Named("value", &namedNumbers)))
		require.Equal(t, numbers, namedNumbers)
		var list struct {
			Value []int32 `sql:"value"`
		}
		require.NoError(t, row.ScanStruct(&list))
		require.Equal(t, numbers, list.Value)

		values := row.Values()
		require.Len(t, values, 1)
		require.Equal(t, `List<Tagged<Int32,"tag">>`, values[0].Type().Yql())
		require.Equal(t, `[AsTagged(42,"tag")]`, values[0].Yql())

		var scanned types.Value
		require.NoError(t, row.Scan(&scanned))
		require.Equal(t, values[0], scanned)
		var named types.Value
		require.NoError(t, row.ScanNamed(query.Named("value", &named)))
		require.Equal(t, values[0], named)
		var dst struct {
			Value types.Value `sql:"value"`
		}
		require.NoError(t, row.ScanStruct(&dst))
		require.Equal(t, values[0], dst.Value)
	})
}
