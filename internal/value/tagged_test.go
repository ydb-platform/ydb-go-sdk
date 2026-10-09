package value

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestTaggedValue(t *testing.T) {
	for _, tt := range []struct {
		v   Value
		yql string
	}{
		{taggedValueForTest(Int32Value(42), "tag"), `AsTagged(42,"tag")`},
		{taggedValueForTest(Int32Value(42), "a\"b\\c"), `AsTagged(42,"a\"b\\c")`},
		{OptionalValue(taggedValueForTest(Int32Value(42), "tag")), `Just(AsTagged(42,"tag"))`},
		{NullValue(types.NewTagged(types.Int32, "tag")), `Nothing(Optional<Tagged<Int32,"tag">>)`},
		{taggedValueForTest(OptionalValue(Int32Value(42)), "tag"), `AsTagged(Just(42),"tag")`},
		{taggedValueForTest(NullValue(types.Int32), "tag"), `AsTagged(Nothing(Optional<Int32>),"tag")`},
		{taggedValueForTest(taggedValueForTest(Int32Value(42), "inner"), "outer"), `AsTagged(AsTagged(42,"inner"),"outer")`},
		{ListValue(taggedValueForTest(Int32Value(42), "tag")), `[AsTagged(42,"tag")]`},
	} {
		t.Run(tt.yql, func(t *testing.T) {
			wire := ToYDB(tt.v)
			decoded := FromYDB(wire.GetType(), wire.GetValue())
			require.True(t, types.Equal(tt.v.Type(), decoded.Type()))
			require.Equal(t, tt.yql, decoded.Yql())
			require.True(t, proto.Equal(wire, ToYDB(decoded)))
			var dst Value
			require.NoError(t, CastTo(decoded, &dst))
			require.Equal(t, tt.yql, dst.Yql())
		})
	}
}

func TestTaggedValueCast(t *testing.T) {
	v := FromYDB(types.NewTagged(types.Int32, "tag").ToYDB(), &Ydb.Value{
		Value: &Ydb.Value_Int32Value{Int32Value: 42},
	})
	var (
		n       int32
		invalid bool
	)
	require.NoError(t, CastTo(v, &n))
	require.EqualValues(t, 42, n)
	require.ErrorIs(t, CastTo(v, &invalid), ErrCannotCast)
	require.Error(t, CastTo(v, n))
	require.Equal(t, `Tagged<Int32,"tag">`, v.Type().Yql())
}

func TestTaggedNestedOptionalValue(t *testing.T) {
	for _, inner := range []types.Type{
		types.NewOptional(types.Int32),
		types.NewTagged(types.NewOptional(types.Int32), "tag"),
		types.NewTagged(types.NewTagged(types.NewOptional(types.Int32), "inner"), "outer"),
	} {
		t.Run(inner.Yql(), func(t *testing.T) {
			wireType := types.NewOptional(inner).ToYDB()
			null := &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}}
			for _, wireValue := range []*Ydb.Value{
				null,
				{Value: &Ydb.Value_NestedValue{NestedValue: null}},
			} {
				decoded := FromYDB(wireType, wireValue)
				require.True(t, proto.Equal(wireType, ToYDB(decoded).GetType()))
				require.True(t, proto.Equal(wireValue, ToYDB(decoded).GetValue()))
			}
		})
	}
}

func TestTaggedEmptyListValue(t *testing.T) {
	wireType := types.NewTagged(types.NewList(types.Int32), "tag").ToYDB()
	wireValue := &Ydb.Value{}
	decoded := FromYDB(wireType, wireValue)
	require.Equal(t, `Tagged<List<Int32>,"tag">`, decoded.Type().Yql())
	require.Equal(t, `AsTagged([],"tag")`, decoded.Yql())
	require.True(t, proto.Equal(wireType, ToYDB(decoded).GetType()))
	require.True(t, proto.Equal(wireValue, ToYDB(decoded).GetValue()))
}

func taggedValueForTest(v Value, tag string) Value {
	return &taggedValue{
		t:     types.NewTagged(v.Type(), tag),
		value: v,
	}
}
