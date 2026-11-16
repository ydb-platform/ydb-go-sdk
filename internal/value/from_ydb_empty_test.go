package value

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestFromYDBEmptyCollections(t *testing.T) {
	for _, want := range []Value{
		ListValue(),
		DictValue(),
		&listValue{t: types.NewList(types.Int32)},
		&dictValue{t: types.NewDict(types.Text, types.Int32)},
		&setValue{t: types.NewSet(types.Text)},
		ListValue(ListValue()),
		ListValue(DictValue()),
		ListValue(&listValue{t: types.NewList(types.Int32)}),
		DictValue(DictValueField{K: TextValue("key"), V: ListValue()}),
		DictValue(DictValueField{K: TextValue("key"), V: &dictValue{t: types.NewDict(types.Text, types.Int32)}}),
		TupleValue(ListValue(), DictValue()),
		StructValue(
			StructValueField{Name: "list", V: ListValue()},
			StructValueField{Name: "dict", V: DictValue()},
		),
		OptionalValue(ListValue()),
		OptionalValue(DictValue()),
		NullValue(types.NewEmptyList()),
		NullValue(types.NewEmptyDict()),
	} {
		t.Run(want.Type().Yql(), func(t *testing.T) {
			wire := ToYDB(want)
			got := FromYDB(wire.GetType(), wire.GetValue())
			require.True(t, types.Equal(want.Type(), got.Type()), got.Type().Yql())
			require.Equal(t, want.Yql(), got.Yql())
			require.True(t, proto.Equal(wire, ToYDB(got)))

			var scanned Value
			require.NoError(t, CastTo(got, &scanned))
			require.Equal(t, got, scanned)

			switch collection := got.(type) {
			case *listValue:
				require.Len(t, collection.ListItems(), len(wire.GetValue().GetItems()))
			case *dictValue:
				require.Len(t, collection.DictValues(), len(wire.GetValue().GetPairs()))
			case *setValue:
				require.Len(t, collection.items, len(wire.GetValue().GetPairs()))
			}
			if !IsNull(got) {
				var incompatible int
				require.ErrorIs(t, CastTo(got, &incompatible), ErrCannotCast)
			}
		})
	}
}

func TestFromYDBOptionalEmptyCollections(t *testing.T) {
	for _, want := range []Value{OptionalValue(ListValue()), OptionalValue(DictValue())} {
		t.Run(want.Type().Yql(), func(t *testing.T) {
			for _, wire := range []*Ydb.Value{
				{},
				{Value: &Ydb.Value_NestedValue{NestedValue: &Ydb.Value{}}},
			} {
				got := FromYDB(want.Type().ToYDB(), wire)
				require.True(t, types.Equal(want.Type(), got.Type()))
				require.Equal(t, want.Yql(), got.Yql())
			}
		})
	}
}
