package value

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"
)

func TestCastToRawValue(t *testing.T) {
	v := Uint64Value(123)
	t.Run("Value", func(t *testing.T) {
		var dst Value
		require.NoError(t, CastTo(v, &dst))
		require.Equal(t, v, dst)
	})
	t.Run("ValuePointer", func(t *testing.T) {
		var dst *Value
		require.NoError(t, CastTo(v, &dst))
		require.NotNil(t, dst)
		require.Equal(t, v, *dst)
	})
	t.Run("ValuePointerReuse", func(t *testing.T) {
		old := Value(TextValue("old"))
		dst := &old
		require.NoError(t, CastTo(v, &dst))
		require.Equal(t, v, *dst)
	})
	t.Run("ProtobufValue", func(t *testing.T) {
		dst := Ydb.Value{Items: []*Ydb.Value{{Value: &Ydb.Value_TextValue{TextValue: "old"}}}}
		require.NoError(t, CastTo(v, &dst))
		require.True(t, proto.Equal(v.toYDB(), &dst))
	})
	t.Run("ProtobufValuePointer", func(t *testing.T) {
		var dst *Ydb.Value
		require.NoError(t, CastTo(v, &dst))
		require.True(t, proto.Equal(v.toYDB(), dst))
	})
	t.Run("ProtobufTypedValue", func(t *testing.T) {
		var dst Ydb.TypedValue
		require.NoError(t, CastTo(v, &dst))
		require.True(t, proto.Equal(v.Type().ToYDB(), dst.GetType()))
		require.True(t, proto.Equal(v.toYDB(), dst.GetValue()))
	})
	t.Run("ProtobufTypedValuePointer", func(t *testing.T) {
		var dst *Ydb.TypedValue
		require.NoError(t, CastTo(v, &dst))
		require.NotNil(t, dst)
		require.True(t, proto.Equal(v.Type().ToYDB(), dst.GetType()))
		require.True(t, proto.Equal(v.toYDB(), dst.GetValue()))
	})
}

func TestCastToRawProtobufDoesNotAliasSource(t *testing.T) {
	for _, tt := range []struct {
		name string
		cast func(t *testing.T, v Value) *Ydb.TypedValue
	}{
		{
			name: "ProtobufValue",
			cast: func(t *testing.T, v Value) *Ydb.TypedValue {
				var dst Ydb.Value
				require.NoError(t, CastTo(v, &dst))

				return &Ydb.TypedValue{Value: &dst}
			},
		},
		{
			name: "ProtobufValuePointer",
			cast: func(t *testing.T, v Value) *Ydb.TypedValue {
				var dst *Ydb.Value
				require.NoError(t, CastTo(v, &dst))
				require.NotNil(t, dst)

				return &Ydb.TypedValue{Value: dst}
			},
		},
		{
			name: "ProtobufTypedValue",
			cast: func(t *testing.T, v Value) *Ydb.TypedValue {
				var dst Ydb.TypedValue
				require.NoError(t, CastTo(v, &dst))

				return &dst
			},
		},
		{
			name: "ProtobufTypedValuePointer",
			cast: func(t *testing.T, v Value) *Ydb.TypedValue {
				var dst *Ydb.TypedValue
				require.NoError(t, CastTo(v, &dst))
				require.NotNil(t, dst)

				return dst
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := ToYDB(ListValue(TextValue("original")))
			original := proto.Clone(raw)
			dst := tt.cast(t, FromProtobuf(raw))
			require.True(t, proto.Equal(raw.GetValue(), dst.GetValue()))
			if dst.GetType() != nil {
				require.True(t, proto.Equal(raw.GetType(), dst.GetType()))
				dst.GetType().GetListType().Item = &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}
			}
			dst.GetValue().GetItems()[0].Value = &Ydb.Value_TextValue{TextValue: "changed"}
			require.True(t, proto.Equal(original, raw), "mutating a raw destination changed its source")
		})
	}
}
