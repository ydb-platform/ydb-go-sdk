package table_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryParameters_String(t *testing.T) {
	for _, tt := range []struct {
		p *table.QueryParameters
		s string
	}{
		{
			p: nil,
			s: `{}`,
		},
		{
			p: table.NewQueryParameters(),
			s: `{}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
			),
			s: `{"$a":"test"u}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
			),
			s: `{"$a":"test"u,"$b":"test"}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
				table.ValueParam("$c", types.Uint64Value(123456)),
			),
			s: `{"$a":"test"u,"$b":"test","$c":123456ul}`,
		},
		{
			p: table.NewQueryParameters(
				table.ValueParam("$a", types.TextValue("test")),
				table.ValueParam("$b", types.BytesValue([]byte("test"))),
				table.ValueParam("$c", types.Uint64Value(123456)),
				table.ValueParam("$d", types.StructValue(
					types.StructFieldValue("$a", types.TextValue("test")),
					types.StructFieldValue("$b", types.BytesValue([]byte("test"))),
					types.StructFieldValue("$c", types.Uint64Value(123456)),
				)),
			),
			s: "{\"$a\":\"test\"u,\"$b\":\"test\",\"$c\":123456ul,\"$d\":<|`$a`:\"test\"u,`$b`:\"test\",`$c`:123456ul|>}",
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.s, tt.p.String())
		})
	}
}
