package types

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func TestTaggedType(t *testing.T) {
	for _, tt := range []struct {
		typeOf Type
		yql    string
	}{
		{NewTagged(Int32, "tag"), `Tagged<Int32,"tag">`},
		{NewTagged(Int32, "a\"b\\c"), `Tagged<Int32,"a\"b\\c">`},
		{NewOptional(NewTagged(Int32, "tag")), `Optional<Tagged<Int32,"tag">>`},
		{NewTagged(NewOptional(Int32), "tag"), `Tagged<Optional<Int32>,"tag">`},
		{NewList(NewTagged(Int32, "tag")), `List<Tagged<Int32,"tag">>`},
		{NewTagged(NewTagged(Int32, "inner"), "outer"), `Tagged<Tagged<Int32,"inner">,"outer">`},
	} {
		t.Run(tt.yql, func(t *testing.T) {
			wire := tt.typeOf.ToYDB()
			decoded := TypeFromYDB(wire)
			require.True(t, Equal(tt.typeOf, decoded))
			require.Equal(t, tt.yql, decoded.Yql())
			require.Equal(t, tt.yql, decoded.String())
			require.True(t, proto.Equal(wire, decoded.ToYDB()))
		})
	}

	wire := &Ydb.Type{Type: &Ydb.Type_TaggedType{TaggedType: &Ydb.TaggedType{
		Tag: "tag", Type: Int32.ToYDB(),
	}}}
	decoded, ok := TypeFromYDB(wire).(*Tagged)
	require.True(t, ok)
	require.Equal(t, "tag", decoded.Tag())
	require.True(t, Equal(Int32, decoded.InnerType()))
	require.False(t, Equal(decoded, Int32))
	require.False(t, Equal(decoded, NewTagged(Int32, "other")))
	require.False(t, Equal(decoded, NewTagged(Int64, "tag")))
}
