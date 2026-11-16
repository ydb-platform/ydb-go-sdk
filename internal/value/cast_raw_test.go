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
