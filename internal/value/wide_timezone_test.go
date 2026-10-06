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
}

func TestWideTimezoneTime(t *testing.T) {
	for _, tt := range []struct {
		id    Ydb.Type_PrimitiveTypeId
		clock string
		hour  int
		min   int
		sec   int
		nanos int
	}{
		{68, "", 0, 0, 0, 0},
		{69, "T12:34:56", 12, 34, 56, 0},
		{70, "T12:34:56.123456", 12, 34, 56, 123456000},
	} {
		wireType := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: tt.id}}
		for _, date := range []struct {
			text  string
			year  int
			month time.Month
			day   int
		}{
			{"-144169-01-01", -144168, time.January, 1},
			{"-0401-02-29", -400, time.February, 29},
			{"-0001-02-29", 0, time.February, 29},
			{"0001-01-01", 1, time.January, 1},
			{"1969-12-31", 1969, time.December, 31},
			{"2024-07-01", 2024, time.July, 1},
			{"10000-02-29", 10000, time.February, 29},
			{"148107-12-31", 148107, time.December, 31},
		} {
			for _, zone := range []string{"UTC", "Europe/Berlin"} {
				text := date.text + tt.clock + "," + zone
				t.Run(types.TypeFromYDB(wireType).Yql()+"/"+text, func(t *testing.T) {
					location, err := time.LoadLocation(zone)
					require.NoError(t, err)
					expected := time.Date(date.year, date.month, date.day, tt.hour, tt.min, tt.sec, tt.nanos, location)
					v := FromYDB(wireType, &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: text}})
					var tm time.Time
					require.NoError(t, CastTo(v, &tm))
					require.Equal(t, expected, tm)
					var sqlValue driver.Value
					require.NoError(t, CastTo(v, &sqlValue))
					require.Equal(t, expected, sqlValue)
					var optional *time.Time
					require.NoError(t, CastTo(OptionalValue(v), &optional))
					require.NotNil(t, optional)
					require.Equal(t, expected, *optional)
					require.NoError(t, CastTo(NullValue(v.Type()), &optional))
					require.Nil(t, optional)
				})
			}
		}
	}
}

func TestWideTimezoneTimeErrors(t *testing.T) {
	for _, tt := range []struct {
		id     Ydb.Type_PrimitiveTypeId
		values []string
	}{
		{68, []string{
			"", "1969-12-31", "1969,UTC", "year-01-01,UTC", "0000-01-01,UTC",
			"10000-02-30,UTC", "-0002-02-29,UTC", "1969-12-31,Invalid/Zone", "1969-12-31,UTC,UTC",
		}},
		{69, []string{"1969-12-31T24:00:00,UTC", "1969-12-31T12:60:00,UTC", "1969-12-31T12:34:60,UTC"}},
		{70, []string{"1969-12-31T12:34:56.invalid,UTC", "148107-02-29T12:34:56,UTC"}},
	} {
		wireType := &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: tt.id}}
		for _, text := range tt.values {
			t.Run(text, func(t *testing.T) {
				v := FromYDB(wireType, &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: text}})
				tm := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
				previous := tm
				require.Error(t, CastTo(v, &tm))
				require.Equal(t, previous, tm)
				var sqlValue driver.Value = "previous value"
				require.Error(t, CastTo(v, &sqlValue))
				require.Equal(t, "previous value", sqlValue)
			})
		}
	}
}
