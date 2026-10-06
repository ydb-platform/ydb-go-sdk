package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/scanner"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestRowTagged(t *testing.T) {
	row := NewRow([]*Ydb.Column{{Name: "value", Type: types.NewTagged(types.Int32, "tag").ToYDB()}}, &Ydb.Value{
		Items: []*Ydb.Value{{Value: &Ydb.Value_Int32Value{Int32Value: 42}}},
	})
	var (
		n       int32
		dst     value.Value
		invalid bool
	)
	require.NoError(t, row.Scan(&n))
	require.EqualValues(t, 42, n)
	require.NoError(t, row.ScanNamed(scanner.NamedRef("value", &n)))
	require.EqualValues(t, 42, n)
	var scalar struct {
		Value int32 `sql:"value"`
	}
	require.NoError(t, row.ScanStruct(&scalar))
	require.EqualValues(t, 42, scalar.Value)
	require.NoError(t, row.Scan(&dst))
	require.True(t, types.Equal(types.NewTagged(types.Int32, "tag"), dst.Type()))
	require.Equal(t, `AsTagged(42,"tag")`, dst.Yql())
	require.Equal(t, dst, row.Values()[0])
	require.NoError(t, row.ScanNamed(scanner.NamedRef("value", &dst)))
	var tagged struct {
		Value value.Value `sql:"value"`
	}
	require.NoError(t, row.ScanStruct(&tagged))
	require.Equal(t, dst, tagged.Value)
	require.ErrorIs(t, row.Scan(&invalid), value.ErrCannotCast)
	require.ErrorIs(t, row.ScanNamed(scanner.NamedRef("value", &invalid)), value.ErrCannotCast)
	var incompatible struct {
		Value bool `sql:"value"`
	}
	require.ErrorIs(t, row.ScanStruct(&incompatible), value.ErrCannotCast)
}
