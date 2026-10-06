package value

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func TestWideTimezoneValues(t *testing.T) {
	for _, tt := range []struct {
		id     Ydb.Type_PrimitiveTypeId
		name   string
		values []string
	}{
		{68, "TzDate32", []string{
			"1969-12-31,Europe/Moscow", "-144169-01-01,UTC", "148107-12-31,UTC", "",
		}},
		{69, "TzDatetime64", []string{
			"1969-12-31T12:34:56,Europe/Moscow", "-144169-01-01T00:00:00,UTC", "148107-12-31T23:59:59,UTC", "",
		}},
		{70, "TzTimestamp64", []string{
			"1969-12-31T12:34:56.123456,Europe/Moscow",
			"-144169-01-01T00:00:00.000000,UTC", "148107-12-31T23:59:59.999999,UTC", "",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			wireType := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: tt.id}}
			for _, text := range tt.values {
				v := FromYDB(wireType, &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: text}})
				require.Equal(t, tt.name, v.Type().Yql())
				require.Equal(t, fmt.Sprintf("%s(%q)", tt.name, text), v.Yql())
				var str string
				require.NoError(t, CastTo(v, &str))
				require.Equal(t, text, str)
				var bytes []byte
				require.NoError(t, CastTo(v, &bytes))
				require.Equal(t, text, string(bytes))
				var scanned Value
				require.NoError(t, CastTo(v, &scanned))
				require.Equal(t, v, scanned)

				for _, value := range []Value{v, OptionalValue(v)} {
					wire := ToYDB(value)
					encoded, err := proto.Marshal(wire)
					require.NoError(t, err)
					var decoded Ydb.TypedValue
					require.NoError(t, proto.Unmarshal(encoded, &decoded))
					require.Equal(t, value, FromYDB(decoded.GetType(), decoded.GetValue()))
				}

				optional := OptionalValue(v)
				var ptr *string
				require.NoError(t, CastTo(optional, &ptr))
				require.NotNil(t, ptr)
				require.Equal(t, text, *ptr)
			}
		})
	}
}

func TestWideTimezoneNullValues(t *testing.T) {
	for _, id := range []Ydb.Type_PrimitiveTypeId{68, 69, 70} {
		wireType := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: id}}
		typ := types.TypeFromYDB(wireType)
		t.Run(typ.Yql(), func(t *testing.T) {
			wire := &Ydb.TypedValue{
				Type:  types.NewOptional(typ).ToYDB(),
				Value: &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}},
			}
			v := FromYDB(wire.GetType(), wire.GetValue())
			require.Equal(t, NullValue(typ), v)
			require.True(t, proto.Equal(wire, ToYDB(v)))
			str := "previous value"
			ptr := &str
			require.NoError(t, CastTo(v, &ptr))
			require.Nil(t, ptr)
			require.Equal(t, fmt.Sprintf("%s(%q)", typ.Yql(), ""), ZeroValue(typ).Yql())
		})
	}
}

func TestWideTimezoneCastErrors(t *testing.T) {
	for _, v := range []Value{
		tzDate32Value("-144169-01-01,UTC"),
		tzDatetime64Value("-144169-01-01T00:00:00,UTC"),
		tzTimestamp64Value("-144169-01-01T00:00:00,UTC"),
	} {
		t.Run(v.Type().Yql(), func(t *testing.T) {
			var n int
			require.ErrorIs(t, CastTo(v, &n), ErrCannotCast)
		})
	}
	for _, text := range []string{
		"-144169-01-01T00:00:00,UTC", "148107-12-31T23:59:59.999999,UTC",
	} {
		var tm time.Time
		var sqlValue driver.Value
		v := tzTimestamp64Value(text)
		require.Error(t, CastTo(v, &tm))
		require.Error(t, CastTo(v, &sqlValue))
	}
}
