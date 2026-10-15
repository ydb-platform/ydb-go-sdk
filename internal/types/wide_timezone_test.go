package types

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func TestWideTimezoneTypes(t *testing.T) {
	for _, tt := range []struct {
		id     Ydb.Type_PrimitiveTypeId
		name   string
		narrow Type
	}{
		{68, "TzDate32", TzDate},
		{69, "TzDatetime64", TzDatetime},
		{70, "TzTimestamp64", TzTimestamp},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wire := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: tt.id}}
			typ := TypeFromYDB(wire)
			require.Equal(t, tt.name, typ.Yql())
			require.Equal(t, tt.name, typ.String())
			require.False(t, Equal(tt.narrow, typ))
			require.True(t, proto.Equal(wire, typ.ToYDB()))

			optional := NewOptional(typ)
			require.Equal(t, "Optional<"+tt.name+">", optional.Yql())
			require.True(t, Equal(optional, TypeFromYDB(optional.ToYDB())))
		})
	}
}
