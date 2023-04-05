package bind

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestToValue(t *testing.T) {
	for _, tt := range []struct {
		src interface{}
		dst types.Value
		err error
	}{
		{
			src: types.BoolValue(true),
			dst: types.BoolValue(true),
			err: nil,
		},

		{
			src: nil,
			dst: types.VoidValue(),
			err: nil,
		},

		{
			src: true,
			dst: types.BoolValue(true),
			err: nil,
		},
		{
			src: func(v bool) *bool { return &v }(true),
			dst: types.OptionalValue(types.BoolValue(true)),
			err: nil,
		},
		{
			src: func() *bool { return nil }(),
			dst: types.NullValue(types.TypeBool),
			err: nil,
		},

		{
			src: 42,
			dst: types.Int32Value(42),
			err: nil,
		},
		{
			src: func(v int) *int { return &v }(42),
			dst: types.OptionalValue(types.Int32Value(42)),
			err: nil,
		},
		{
			src: func() *int { return nil }(),
			dst: types.NullValue(types.TypeInt32),
			err: nil,
		},

		{
			src: uint(42),
			dst: types.Uint32Value(42),
			err: nil,
		},
		{
			src: func(v uint) *uint { return &v }(42),
			dst: types.OptionalValue(types.Uint32Value(42)),
			err: nil,
		},
		{
			src: func() *uint { return nil }(),
			dst: types.NullValue(types.TypeUint32),
			err: nil,
		},

		{
			src: int8(42),
			dst: types.Int8Value(42),
			err: nil,
		},
		{
			src: func(v int8) *int8 { return &v }(42),
			dst: types.OptionalValue(types.Int8Value(42)),
			err: nil,
		},
		{
			src: func() *int8 { return nil }(),
			dst: types.NullValue(types.TypeInt8),
			err: nil,
		},

		{
			src: uint8(42),
			dst: types.Uint8Value(42),
			err: nil,
		},
		{
			src: func(v uint8) *uint8 { return &v }(42),
			dst: types.OptionalValue(types.Uint8Value(42)),
			err: nil,
		},
		{
			src: func() *uint8 { return nil }(),
			dst: types.NullValue(types.TypeUint8),
			err: nil,
		},

		{
			src: int16(42),
			dst: types.Int16Value(42),
			err: nil,
		},
		{
			src: func(v int16) *int16 { return &v }(42),
			dst: types.OptionalValue(types.Int16Value(42)),
			err: nil,
		},
		{
			src: func() *int16 { return nil }(),
			dst: types.NullValue(types.TypeInt16),
			err: nil,
		},

		{
			src: uint16(42),
			dst: types.Uint16Value(42),
			err: nil,
		},
		{
			src: func(v uint16) *uint16 { return &v }(42),
			dst: types.OptionalValue(types.Uint16Value(42)),
			err: nil,
		},
		{
			src: func() *uint16 { return nil }(),
			dst: types.NullValue(types.TypeUint16),
			err: nil,
		},

		{
			src: int32(42),
			dst: types.Int32Value(42),
			err: nil,
		},
		{
			src: func(v int32) *int32 { return &v }(42),
			dst: types.OptionalValue(types.Int32Value(42)),
			err: nil,
		},
		{
			src: func() *int32 { return nil }(),
			dst: types.NullValue(types.TypeInt32),
			err: nil,
		},

		{
			src: uint32(42),
			dst: types.Uint32Value(42),
			err: nil,
		},
		{
			src: func(v uint32) *uint32 { return &v }(42),
			dst: types.OptionalValue(types.Uint32Value(42)),
			err: nil,
		},
		{
			src: func() *uint32 { return nil }(),
			dst: types.NullValue(types.TypeUint32),
			err: nil,
		},

		{
			src: int64(42),
			dst: types.Int64Value(42),
			err: nil,
		},
		{
			src: func(v int64) *int64 { return &v }(42),
			dst: types.OptionalValue(types.Int64Value(42)),
			err: nil,
		},
		{
			src: func() *int64 { return nil }(),
			dst: types.NullValue(types.TypeInt64),
			err: nil,
		},

		{
			src: uint64(42),
			dst: types.Uint64Value(42),
			err: nil,
		},
		{
			src: func(v uint64) *uint64 { return &v }(42),
			dst: types.OptionalValue(types.Uint64Value(42)),
			err: nil,
		},
		{
			src: func() *uint64 { return nil }(),
			dst: types.NullValue(types.TypeUint64),
			err: nil,
		},

		{
			src: float32(42),
			dst: types.FloatValue(42),
			err: nil,
		},
		{
			src: func(v float32) *float32 { return &v }(42),
			dst: types.OptionalValue(types.FloatValue(42)),
			err: nil,
		},
		{
			src: func() *float32 { return nil }(),
			dst: types.NullValue(types.TypeFloat),
			err: nil,
		},

		{
			src: float64(42),
			dst: types.DoubleValue(42),
			err: nil,
		},
		{
			src: func(v float64) *float64 { return &v }(42),
			dst: types.OptionalValue(types.DoubleValue(42)),
			err: nil,
		},
		{
			src: func() *float64 { return nil }(),
			dst: types.NullValue(types.TypeDouble),
			err: nil,
		},

		{
			src: "test",
			dst: types.TextValue("test"),
			err: nil,
		},
		{
			src: func(v string) *string { return &v }("test"),
			dst: types.OptionalValue(types.TextValue("test")),
			err: nil,
		},
		{
			src: func() *string { return nil }(),
			dst: types.NullValue(types.TypeText),
			err: nil,
		},

		{
			src: []byte("test"),
			dst: types.BytesValue([]byte("test")),
			err: nil,
		},
		{
			src: func(v []byte) *[]byte { return &v }([]byte("test")),
			dst: types.OptionalValue(types.BytesValue([]byte("test"))),
			err: nil,
		},
		{
			src: func() *[]byte { return nil }(),
			dst: types.NullValue(types.TypeBytes),
			err: nil,
		},

		{
			src: []string{"test"},
			dst: types.ListValue(types.TextValue("test")),
			err: nil,
		},

		{
			src: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst: types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			err: nil,
		},
		{
			src: func(v [16]byte) *[16]byte { return &v }([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			dst: types.OptionalValue(types.UUIDValue([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
			err: nil,
		},
		{
			src: func() *[16]byte { return nil }(),
			dst: types.NullValue(types.TypeUUID),
			err: nil,
		},

		{
			src: time.Unix(42, 43),
			dst: types.TimestampValueFromTime(time.Unix(42, 43)),
			err: nil,
		},
		{
			src: func(v time.Time) *time.Time { return &v }(time.Unix(42, 43)),
			dst: types.OptionalValue(types.TimestampValueFromTime(time.Unix(42, 43))),
			err: nil,
		},
		{
			src: func() *time.Time { return nil }(),
			dst: types.NullValue(types.TypeTimestamp),
			err: nil,
		},

		{
			src: time.Duration(42),
			dst: types.IntervalValueFromDuration(time.Duration(42)),
			err: nil,
		},
		{
			src: func(v time.Duration) *time.Duration { return &v }(time.Duration(42)),
			dst: types.OptionalValue(types.IntervalValueFromDuration(time.Duration(42))),
			err: nil,
		},
		{
			src: func() *time.Duration { return nil }(),
			dst: types.NullValue(types.TypeInterval),
			err: nil,
		},
	} {
		t.Run(fmt.Sprintf("%T(%v)", tt.src, tt.src), func(t *testing.T) {
			dst, err := toValue(tt.src)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.Equal(t, tt.dst, dst)
			}
		})
	}
}

func named(name string, value interface{}) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

func TestYdbParam(t *testing.T) {
	for _, tt := range []struct {
		src interface{}
		dst table.ParameterOption
		err error
	}{
		{
			src: table.ValueParam("$a", types.Int32Value(42)),
			dst: table.ValueParam("$a", types.Int32Value(42)),
			err: nil,
		},
		{
			src: named("a", int(42)),
			dst: table.ValueParam("$a", types.Int32Value(42)),
			err: nil,
		},
		{
			src: named("$a", int(42)),
			dst: table.ValueParam("$a", types.Int32Value(42)),
			err: nil,
		},
		{
			src: named("a", uint(42)),
			dst: table.ValueParam("$a", types.Uint32Value(42)),
			err: nil,
		},
		{
			src: driver.NamedValue{Value: uint(42)},
			dst: nil,
			err: errUnnamedParam,
		},
	} {
		t.Run("", func(t *testing.T) {
			dst, err := toYdbParam("", tt.src)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.Equal(t, tt.dst, dst)
			}
		})
	}
}

func TestArgsToParams(t *testing.T) {
	for _, tt := range []struct {
		args   []interface{}
		params []table.ParameterOption
		err    error
	}{
		{
			args:   []interface{}{},
			params: []table.ParameterOption{},
			err:    nil,
		},
		{
			args: []interface{}{
				1, uint64(2), "3",
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				table.NewQueryParameters(
					table.ValueParam("$p0", types.Int32Value(1)),
					table.ValueParam("$p1", types.Uint64Value(2)),
					table.ValueParam("$p2", types.TextValue("3")),
				),
				table.NewQueryParameters(
					table.ValueParam("$p0", types.Int32Value(1)),
					table.ValueParam("$p1", types.Uint64Value(2)),
					table.ValueParam("$p2", types.TextValue("3")),
				),
			},
			err: errMultipleQueryParameters,
		},
		{
			args: []interface{}{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				sql.Named("$p0", types.Int32Value(1)),
				sql.Named("$p1", types.Uint64Value(2)),
				sql.Named("$p2", types.TextValue("3")),
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				driver.NamedValue{Name: "$p0", Value: types.Int32Value(1)},
				driver.NamedValue{Name: "$p1", Value: types.Uint64Value(2)},
				driver.NamedValue{Name: "$p2", Value: types.TextValue("3")},
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				driver.NamedValue{Value: table.ValueParam("$p0", types.Int32Value(1))},
				driver.NamedValue{Value: table.ValueParam("$p1", types.Uint64Value(2))},
				driver.NamedValue{Value: table.ValueParam("$p2", types.TextValue("3"))},
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				driver.NamedValue{Value: 1},
				driver.NamedValue{Value: uint64(2)},
				driver.NamedValue{Value: "3"},
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				driver.NamedValue{Value: table.NewQueryParameters(
					table.ValueParam("$p0", types.Int32Value(1)),
					table.ValueParam("$p1", types.Uint64Value(2)),
					table.ValueParam("$p2", types.TextValue("3")),
				)},
			},
			params: []table.ParameterOption{
				table.ValueParam("$p0", types.Int32Value(1)),
				table.ValueParam("$p1", types.Uint64Value(2)),
				table.ValueParam("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			args: []interface{}{
				driver.NamedValue{Value: table.NewQueryParameters(
					table.ValueParam("$p0", types.Int32Value(1)),
					table.ValueParam("$p1", types.Uint64Value(2)),
					table.ValueParam("$p2", types.TextValue("3")),
				)},
				driver.NamedValue{Value: table.ValueParam("$p1", types.Uint64Value(2))},
				driver.NamedValue{Value: table.ValueParam("$p2", types.TextValue("3"))},
			},
			err: errMultipleQueryParameters,
		},
	} {
		t.Run("", func(t *testing.T) {
			params, err := Params(tt.args...)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.params, params)
			}
		})
	}
}
