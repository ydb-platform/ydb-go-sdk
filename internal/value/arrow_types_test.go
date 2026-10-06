package value

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestNullableWireValues(t *testing.T) {
	null := &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}}
	number := &Ydb.Value{Value: &Ydb.Value_Int32Value{Int32Value: 42}}
	nested := func(v *Ydb.Value) *Ydb.Value { return &Ydb.Value{Value: &Ydb.Value_NestedValue{NestedValue: v}} }
	variant := types.NewVariantTuple(types.NewOptional(types.Int32), types.Text)
	for _, test := range []struct {
		name  string
		value Value
		wire  *Ydb.Value
	}{
		{"literal null", LiteralNullValue(), null},
		{"null optional", NullValue(types.Int32), null},
		{"present null", OptionalValue(NullValue(types.Int32)), nested(null)},
		{"twice present null", OptionalValue(OptionalValue(NullValue(types.Int32))), nested(nested(null))},
		{"present nested scalar", OptionalValue(OptionalValue(Int32Value(42))), number},
		{"null variant payload", VariantValueTuple(NullValue(types.Int32), 0, variant), nested(null)},
		{"present null variant", OptionalValue(VariantValueTuple(NullValue(types.Int32), 0, variant)), nested(nested(null))},
		{
			"twice present null variant",
			OptionalValue(OptionalValue(VariantValueTuple(NullValue(types.Int32), 0, variant))),
			nested(nested(nested(null))),
		},
		{"present variant", OptionalValue(VariantValueTuple(OptionalValue(Int32Value(42)), 0, variant)), nested(number)},
		{
			"twice present variant",
			OptionalValue(OptionalValue(VariantValueTuple(OptionalValue(Int32Value(42)), 0, variant))),
			nested(number),
		},
		{"pg null", PgNullValue(23), null},
		{"present pg null", OptionalValue(PgNullValue(23)), nested(null)},
		{"tagged null", TaggedValue(types.NewTagged(types.NewOptional(types.Int32), "tag"), NullValue(types.Int32)), null},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := ToYDB(test.value)
			require.True(t, proto.Equal(encoded.GetValue(), test.wire), "got %s, want %s", encoded.GetValue(), test.wire)
			decoded, err := fromYDB(encoded.GetType(), test.wire)
			require.NoError(t, err)
			require.True(t, types.Equal(decoded.Type(), test.value.Type()))
			require.Equal(t, test.value.Yql(), decoded.Yql())
			require.True(t, proto.Equal(ToYDB(decoded), encoded))
		})
	}
}

func TestAdditionalYQLValues(t *testing.T) {
	for _, v := range []Value{
		ListValue(), DictValue(),
		ListValueWithType(types.NewList(types.Int32), nil),
		DictValueWithType(types.NewDict(types.Bytes, types.Int32), nil),
		SetValueWithType(types.NewSet(types.Bytes), nil),
		TaggedValue(types.NewTagged(types.Int32, "tag"), Int32Value(42)),
		TzDate32Value( "1969-12-31,Europe/Moscow"),
		TzDatetime64Value( "1969-12-31T12:34:56,Europe/Moscow"),
		TzTimestamp64Value( "1969-12-31T12:34:56.123456,Europe/Moscow"),
		PgValue(25, ""), PgNullValue(25),
	} {
		t.Run(v.Type().Yql()+"/"+v.Yql(), func(t *testing.T) {
			encoded := ToYDB(v)
			decoded, err := fromYDB(encoded.GetType(), encoded.GetValue())
			require.NoError(t, err)
			require.True(t, types.Equal(v.Type(), decoded.Type()))
			require.Equal(t, v.Yql(), decoded.Yql())
			require.True(t, proto.Equal(encoded, ToYDB(decoded)))
		})
	}
	var target driver.Value = "old"
	require.NoError(t, CastTo(LiteralNullValue(), &target))
	require.Nil(t, target)
	require.False(t, proto.Equal(ToYDB(PgValue(25, "")), ToYDB(PgNullValue(25))))
}
