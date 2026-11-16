package bind

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

func TestAutoDeclareMixedNullParameters(t *testing.T) {
	const body = "SELECT $none, $id, $typed, $another, '$none' -- $id"
	yql, bound, err := (Bindings{AutoDeclare{}}).ToYdb(body,
		sql.Named("typed", (*string)(nil)), sql.Named("none", nil), sql.Named("id", int64(7)), sql.Named("another", nil),
	)
	require.NoError(t, err)
	require.Equal(t, "-- bind declares\nDECLARE $id AS Int64;\nDECLARE $typed AS Optional<Utf8>;\n"+
		"$another = NULL;\n$none = NULL;\n\n"+body, yql)
	wire, err := bound.ToYDB()
	require.NoError(t, err)
	require.Len(t, wire, 2)
	require.NotContains(t, wire, "$none")
	require.NotContains(t, wire, "$another")
	require.EqualValues(t, 7, wire["$id"].GetValue().GetInt64Value())
	require.Equal(t, Ydb.Type_UTF8, wire["$typed"].GetType().GetOptionalType().GetItem().GetTypeId())
	require.IsType(t, &Ydb.Value_NullFlagValue{}, wire["$typed"].GetValue().GetValue())
}

func TestAutoDeclarePositionalNull(t *testing.T) {
	bindings := Bindings(Sort([]Bind{AutoDeclare{}, PositionalArgs{}}))
	yql, bound, err := bindings.ToYdb("SELECT ?, ?", nil, int64(7))
	require.NoError(t, err)
	require.Equal(t, "-- bind declares\nDECLARE $p1 AS Int64;\n$p0 = NULL;\n\n"+
		"-- origin query with positional args replacement\nSELECT $p0, $p1", yql)
	wire, err := bound.ToYDB()
	require.NoError(t, err)
	require.Len(t, wire, 1)
	require.NotContains(t, wire, "$p0")
	require.EqualValues(t, 7, wire["$p1"].GetValue().GetInt64Value())
}
