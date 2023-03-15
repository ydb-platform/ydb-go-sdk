package table

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestQueryParameters_String(t *testing.T) {
	for _, tt := range []struct {
		p *QueryParameters
		s string
	}{
		{
			p: nil,
			s: `{}`,
		},
		{
			p: NewQueryParameters(),
			s: `{}`,
		},
		{
			p: NewQueryParameters(
				ValueParam("$a", types.TextValue("test")),
			),
			s: `{"$a":"test"u}`,
		},
		{
			p: NewQueryParameters(
				ValueParam("$a", types.TextValue("test")),
				ValueParam("$b", types.BytesValue([]byte("test"))),
			),
			s: `{"$a":"test"u,"$b":"test"}`,
		},
		{
			p: NewQueryParameters(
				ValueParam("$a", types.TextValue("test")),
				ValueParam("$b", types.BytesValue([]byte("test"))),
				ValueParam("$c", types.Uint64Value(123456)),
			),
			s: `{"$a":"test"u,"$b":"test","$c":123456ul}`,
		},
		{
			p: NewQueryParameters(
				ValueParam("$a", types.TextValue("test")),
				ValueParam("$b", types.BytesValue([]byte("test"))),
				ValueParam("$c", types.Uint64Value(123456)),
				ValueParam("$d", types.StructValue(
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
