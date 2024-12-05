package bind

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestToValue(t *testing.T) {
	for _, tt := range []struct {
		name string
		src  interface{}
		dst  types.Value
		err  error
	}{
		{
			name: xtest.CurrentFileLine(),
			src:  types.BoolValue(true),
			dst:  types.BoolValue(true),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  nil,
			dst:  types.VoidValue(),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  true,
			dst:  types.BoolValue(true),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v bool) *bool { return &v }(true),
			dst:  types.OptionalValue(types.BoolValue(true)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *bool { return nil }(),
			dst:  types.NullValue(types.TypeBool),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  42,
			dst:  types.Int32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int) *int { return &v }(42),
			dst:  types.OptionalValue(types.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int { return nil }(),
			dst:  types.NullValue(types.TypeInt32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint(42),
			dst:  types.Uint32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint) *uint { return &v }(42),
			dst:  types.OptionalValue(types.Uint32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint { return nil }(),
			dst:  types.NullValue(types.TypeUint32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int8(42),
			dst:  types.Int8Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int8) *int8 { return &v }(42),
			dst:  types.OptionalValue(types.Int8Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int8 { return nil }(),
			dst:  types.NullValue(types.TypeInt8),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint8(42),
			dst:  types.Uint8Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint8) *uint8 { return &v }(42),
			dst:  types.OptionalValue(types.Uint8Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint8 { return nil }(),
			dst:  types.NullValue(types.TypeUint8),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int16(42),
			dst:  types.Int16Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int16) *int16 { return &v }(42),
			dst:  types.OptionalValue(types.Int16Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int16 { return nil }(),
			dst:  types.NullValue(types.TypeInt16),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint16(42),
			dst:  types.Uint16Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint16) *uint16 { return &v }(42),
			dst:  types.OptionalValue(types.Uint16Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint16 { return nil }(),
			dst:  types.NullValue(types.TypeUint16),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int32(42),
			dst:  types.Int32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int32) *int32 { return &v }(42),
			dst:  types.OptionalValue(types.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int32 { return nil }(),
			dst:  types.NullValue(types.TypeInt32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint32(42),
			dst:  types.Uint32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint32) *uint32 { return &v }(42),
			dst:  types.OptionalValue(types.Uint32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint32 { return nil }(),
			dst:  types.NullValue(types.TypeUint32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int64(42),
			dst:  types.Int64Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int64) *int64 { return &v }(42),
			dst:  types.OptionalValue(types.Int64Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int64 { return nil }(),
			dst:  types.NullValue(types.TypeInt64),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint64(42),
			dst:  types.Uint64Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint64) *uint64 { return &v }(42),
			dst:  types.OptionalValue(types.Uint64Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint64 { return nil }(),
			dst:  types.NullValue(types.TypeUint64),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  float32(42),
			dst:  types.FloatValue(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v float32) *float32 { return &v }(42),
			dst:  types.OptionalValue(types.FloatValue(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *float32 { return nil }(),
			dst:  types.NullValue(types.TypeFloat),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  float64(42),
			dst:  types.DoubleValue(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v float64) *float64 { return &v }(42),
			dst:  types.OptionalValue(types.DoubleValue(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *float64 { return nil }(),
			dst:  types.NullValue(types.TypeDouble),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  "test",
			dst:  types.TextValue("test"),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v string) *string { return &v }("test"),
			dst:  types.OptionalValue(types.TextValue("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *string { return nil }(),
			dst:  types.NullValue(types.TypeText),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []byte("test"),
			dst:  types.BytesValue([]byte("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v []byte) *[]byte { return &v }([]byte("test")),
			dst:  types.OptionalValue(types.BytesValue([]byte("test"))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *[]byte { return nil }(),
			dst:  types.NullValue(types.TypeBytes),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []string{"test"},
			dst:  types.ListValue(types.TextValue("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst:  nil,
			err:  types.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *[16]byte { return nil }(),
			dst:  nil,
			err:  types.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v [16]byte) *[16]byte { return &v }([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			dst:  nil,
			err:  types.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst:  types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  &uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			// uuid implemented driver.Valuer and doesn't set optional wrapper
			dst: types.OptionalValue(types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
			err: nil,
		},
		// https://github.com/ydb-platform/ydb-go-sdk/issues/1515
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uuid.UUID { return nil }(),
			dst:  types.NullValue(types.TypeUUID),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  time.Unix(42, 43),
			dst:  types.TimestampValueFromTime(time.Unix(42, 43)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v time.Time) *time.Time { return &v }(time.Unix(42, 43)),
			dst:  types.OptionalValue(types.TimestampValueFromTime(time.Unix(42, 43))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *time.Time { return nil }(),
			dst:  types.NullValue(types.TypeTimestamp),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  time.Duration(42),
			dst:  types.IntervalValueFromDuration(time.Duration(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v time.Duration) *time.Duration { return &v }(time.Duration(42)),
			dst:  types.OptionalValue(types.IntervalValueFromDuration(time.Duration(42))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *time.Duration { return nil }(),
			dst:  types.NullValue(types.TypeInterval),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: &struct {
				A string   `sql:"a"`
				B uint64   `sql:"b"`
				C []string `sql:"c"`
			}{
				A: "a",
				B: 123,
				C: []string{"1", "2", "3"},
			},
			dst: types.OptionalValue(types.StructValue(
				types.StructFieldValue("a", types.TextValue("a")),
				types.StructFieldValue("b", types.Uint64Value(123)),
				types.StructFieldValue("c", types.ListValue(types.TextValue("1"), types.TextValue("2"), types.TextValue("3"))),
			)),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []uint64{123, 123, 123, 123, 123, 123},
			dst: types.ListValue(
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
			),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: []value.Value{
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
			},
			dst: types.ListValue(
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
				types.Uint64Value(123),
			),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: struct {
				A string   `sql:"a"`
				B uint64   `sql:"b"`
				C []string `sql:"c"`
			}{
				A: "a",
				B: 123,
				C: []string{"1", "2", "3"},
			},
			dst: types.StructValue(
				types.StructFieldValue("a", types.TextValue("a")),
				types.StructFieldValue("b", types.Uint64Value(123)),
				types.StructFieldValue("c", types.ListValue(types.TextValue("1"), types.TextValue("2"), types.TextValue("3"))),
			),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: &struct {
				A string   `sql:"a"`
				B uint64   `sql:"b"`
				C []string `sql:"c"`
			}{
				A: "a",
				B: 123,
				C: []string{"1", "2", "3"},
			},
			dst: types.OptionalValue(types.StructValue(
				types.StructFieldValue("a", types.TextValue("a")),
				types.StructFieldValue("b", types.Uint64Value(123)),
				types.StructFieldValue("c", types.ListValue(types.TextValue("1"), types.TextValue("2"), types.TextValue("3"))),
			)),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: (*struct {
				A string   `sql:"a"`
				B uint64   `sql:"b"`
				C []string `sql:"c"`
			})(nil),
			dst: types.NullValue(types.Struct(
				types.StructField("a", types.TypeText),
				types.StructField("b", types.TypeUint64),
				types.StructField("c", types.List(types.TypeText)),
			)),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: map[uint64]any{
				1: "1",
				2: uint64(2),
				3: []*uuid.UUID{
					{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
					{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48},
				},
			},
			dst: types.DictValue(
				types.DictFieldValue(types.Uint64Value(1), types.TextValue("1")),
				types.DictFieldValue(types.Uint64Value(2), types.Uint64Value(2)),
				types.DictFieldValue(types.Uint64Value(3), types.ListValue(
					types.OptionalValue(types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
					types.OptionalValue(types.UuidValue(uuid.UUID{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})),
					types.OptionalValue(types.UuidValue(uuid.UUID{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})),
				)),
			),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: &map[uint64]any{
				1: "1",
				2: uint64(2),
				3: []*uuid.UUID{
					{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
					{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
					{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48},
				},
			},
			dst: types.OptionalValue(types.DictValue(
				types.DictFieldValue(types.Uint64Value(1), types.TextValue("1")),
				types.DictFieldValue(types.Uint64Value(2), types.Uint64Value(2)),
				types.DictFieldValue(types.Uint64Value(3), types.ListValue(
					types.OptionalValue(types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
					types.OptionalValue(types.UuidValue(uuid.UUID{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})),
					types.OptionalValue(types.UuidValue(uuid.UUID{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})),
				)),
			)),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  (*map[uint64]any)(nil),
			dst:  nil,
			err:  errUnsupportedType,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst:  nil,
			err:  value.ErrIssue1501BadUUID,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dst, err := toValue(tt.src)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.Equal(t, tt.dst.Yql(), dst.Yql())
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
		name string
		src  interface{}
		dst  *params.Parameter
		err  error
	}{
		{
			name: xtest.CurrentFileLine(),
			src:  params.Named("$a", types.Int32Value(42)),
			dst:  params.Named("$a", types.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("a", int(42)),
			dst:  params.Named("$a", types.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("$a", int(42)),
			dst:  params.Named("$a", types.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("a", uint(42)),
			dst:  params.Named("$a", types.Uint32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  driver.NamedValue{Value: uint(42)},
			dst:  nil,
			err:  errUnnamedParam,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
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
		name   string
		args   []interface{}
		params []*params.Parameter
		err    error
	}{
		{
			name:   xtest.CurrentFileLine(),
			args:   []interface{}{},
			params: []*params.Parameter{},
			err:    nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				1, uint64(2), "3",
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				table.NewQueryParameters(
					params.Named("$p0", types.Int32Value(1)),
					params.Named("$p1", types.Uint64Value(2)),
					params.Named("$p2", types.TextValue("3")),
				),
				table.NewQueryParameters(
					params.Named("$p0", types.Int32Value(1)),
					params.Named("$p1", types.Uint64Value(2)),
					params.Named("$p2", types.TextValue("3")),
				),
			},
			err: errMultipleQueryParameters,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				sql.Named("$p0", types.Int32Value(1)),
				sql.Named("$p1", types.Uint64Value(2)),
				sql.Named("$p2", types.TextValue("3")),
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				driver.NamedValue{Name: "$p0", Value: types.Int32Value(1)},
				driver.NamedValue{Name: "$p1", Value: types.Uint64Value(2)},
				driver.NamedValue{Name: "$p2", Value: types.TextValue("3")},
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				driver.NamedValue{Value: params.Named("$p0", types.Int32Value(1))},
				driver.NamedValue{Value: params.Named("$p1", types.Uint64Value(2))},
				driver.NamedValue{Value: params.Named("$p2", types.TextValue("3"))},
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				driver.NamedValue{Value: 1},
				driver.NamedValue{Value: uint64(2)},
				driver.NamedValue{Value: "3"},
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				driver.NamedValue{Value: table.NewQueryParameters(
					params.Named("$p0", types.Int32Value(1)),
					params.Named("$p1", types.Uint64Value(2)),
					params.Named("$p2", types.TextValue("3")),
				)},
			},
			params: []*params.Parameter{
				params.Named("$p0", types.Int32Value(1)),
				params.Named("$p1", types.Uint64Value(2)),
				params.Named("$p2", types.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []interface{}{
				driver.NamedValue{Value: table.NewQueryParameters(
					params.Named("$p0", types.Int32Value(1)),
					params.Named("$p1", types.Uint64Value(2)),
					params.Named("$p2", types.TextValue("3")),
				)},
				driver.NamedValue{Value: params.Named("$p1", types.Uint64Value(2))},
				driver.NamedValue{Value: params.Named("$p2", types.TextValue("3"))},
			},
			err: errMultipleQueryParameters,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
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

func TestAsUUID(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		t.Run("uuid.UUID", func(t *testing.T) {
			v, ok := asUUID(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			require.True(t, ok)
			require.Equal(t, types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), v)
		})
		t.Run("*uuid.UUID", func(t *testing.T) {
			v, ok := asUUID(&uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			require.True(t, ok)
			require.Equal(t, types.OptionalValue(types.UuidValue(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})), v) //nolint:lll
		})
	})
	t.Run("Invalid", func(t *testing.T) {
		v, ok := asUUID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		require.False(t, ok)
		require.Nil(t, v)
	})
}

func asUUIDForceTypeCast(v interface{}) (value.Value, bool) {
	return value.Uuid(v.(uuid.UUID)), true
}

func BenchmarkAsUUIDForceTypeCast(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, ok := asUUIDForceTypeCast(srcUUID)
		require.True(b, ok)
		require.Equal(b, expUUIDValue, v)
	}
}

func BenchmarkAsUUID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, ok := asUUID(srcUUID)
		require.True(b, ok)
		require.Equal(b, expUUIDValue, v)
	}
}

var (
	uuidType     = reflect.TypeOf(uuid.UUID{})
	uuidPtrType  = reflect.TypeOf(&uuid.UUID{})
	srcUUID      = uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	expUUIDValue = value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
)

func asUUIDUsingReflect(v interface{}) (value.Value, bool) {
	switch reflect.TypeOf(v) {
	case uuidType:
		return value.Uuid(v.(uuid.UUID)), true
	case uuidPtrType:
		if v == nil {
			return value.NullValue(types.TypeUUID), false
		}

		return value.OptionalValue(value.Uuid(*(v.(*uuid.UUID)))), true
	}

	return nil, false
}

func TestAsUUIDUsingReflect(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		t.Run("uuid.UUID", func(t *testing.T) {
			v, ok := asUUIDUsingReflect(srcUUID)
			require.True(t, ok)
			require.Equal(t, expUUIDValue, v)
		})
		t.Run("*uuid.UUID", func(t *testing.T) {
			v, ok := asUUIDUsingReflect(&srcUUID)
			require.True(t, ok)
			require.Equal(t, value.OptionalValue(expUUIDValue), v)
		})
	})
	t.Run("Invalid", func(t *testing.T) {
		t.Run("[16]byte", func(t *testing.T) {
			v, ok := asUUIDUsingReflect(([16]byte)(srcUUID))
			require.False(t, ok)
			require.Nil(t, v)
		})
		t.Run("*[16]byte", func(t *testing.T) {
			v, ok := asUUIDUsingReflect((*[16]byte)(&srcUUID))
			require.False(t, ok)
			require.Nil(t, v)
		})
	})
}

func BenchmarkAsUUIDUsingReflect(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, ok := asUUIDUsingReflect(srcUUID)
		require.True(b, ok)
		require.Equal(b, expUUIDValue, v)
	}
}
