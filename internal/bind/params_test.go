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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type testValuer struct {
	value driver.Value
}

func (v testValuer) Value() (driver.Value, error) {
	return v.value, nil
}

func TestToValue(t *testing.T) {
	for _, tt := range []struct {
		name string
		src  any
		dst  value.Value
		err  error
	}{
		{
			name: xtest.CurrentFileLine(),
			src:  value.BoolValue(true),
			dst:  value.BoolValue(true),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  nil,
			dst:  value.VoidValue(),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  true,
			dst:  value.BoolValue(true),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v bool) *bool { return &v }(true),
			dst:  value.OptionalValue(value.BoolValue(true)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *bool { return nil }(),
			dst:  value.NullValue(types.Bool),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  42,
			dst:  value.Int32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int) *int { return &v }(42),
			dst:  value.OptionalValue(value.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int { return nil }(),
			dst:  value.NullValue(types.Int32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint(42),
			dst:  value.Uint32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint) *uint { return &v }(42),
			dst:  value.OptionalValue(value.Uint32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint { return nil }(),
			dst:  value.NullValue(types.Uint32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int8(42),
			dst:  value.Int8Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int8) *int8 { return &v }(42),
			dst:  value.OptionalValue(value.Int8Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int8 { return nil }(),
			dst:  value.NullValue(types.Int8),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint8(42),
			dst:  value.Uint8Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint8) *uint8 { return &v }(42),
			dst:  value.OptionalValue(value.Uint8Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint8 { return nil }(),
			dst:  value.NullValue(types.Uint8),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int16(42),
			dst:  value.Int16Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int16) *int16 { return &v }(42),
			dst:  value.OptionalValue(value.Int16Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int16 { return nil }(),
			dst:  value.NullValue(types.Int16),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint16(42),
			dst:  value.Uint16Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint16) *uint16 { return &v }(42),
			dst:  value.OptionalValue(value.Uint16Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint16 { return nil }(),
			dst:  value.NullValue(types.Uint16),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int32(42),
			dst:  value.Int32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int32) *int32 { return &v }(42),
			dst:  value.OptionalValue(value.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int32 { return nil }(),
			dst:  value.NullValue(types.Int32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint32(42),
			dst:  value.Uint32Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint32) *uint32 { return &v }(42),
			dst:  value.OptionalValue(value.Uint32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint32 { return nil }(),
			dst:  value.NullValue(types.Uint32),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  int64(42),
			dst:  value.Int64Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v int64) *int64 { return &v }(42),
			dst:  value.OptionalValue(value.Int64Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *int64 { return nil }(),
			dst:  value.NullValue(types.Int64),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uint64(42),
			dst:  value.Uint64Value(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v uint64) *uint64 { return &v }(42),
			dst:  value.OptionalValue(value.Uint64Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uint64 { return nil }(),
			dst:  value.NullValue(types.Uint64),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  float32(42),
			dst:  value.FloatValue(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v float32) *float32 { return &v }(42),
			dst:  value.OptionalValue(value.FloatValue(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *float32 { return nil }(),
			dst:  value.NullValue(types.Float),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  float64(42),
			dst:  value.DoubleValue(42),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v float64) *float64 { return &v }(42),
			dst:  value.OptionalValue(value.DoubleValue(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *float64 { return nil }(),
			dst:  value.NullValue(types.Double),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  "test",
			dst:  value.TextValue("test"),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v string) *string { return &v }("test"),
			dst:  value.OptionalValue(value.TextValue("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *string { return nil }(),
			dst:  value.NullValue(types.Text),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []byte("test"),
			dst:  value.BytesValue([]byte("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v []byte) *[]byte { return &v }([]byte("test")),
			dst:  value.OptionalValue(value.BytesValue([]byte("test"))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *[]byte { return nil }(),
			dst:  value.NullValue(types.Bytes),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []string{"test"},
			dst:  value.ListValue(value.TextValue("test")),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst:  nil,
			err:  value.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *[16]byte { return nil }(),
			dst:  nil,
			err:  value.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v [16]byte) *[16]byte { return &v }([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			dst:  nil,
			err:  value.ErrIssue1501BadUUID,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			dst:  value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  &uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			// uuid implemented driver.Valuer and doesn't set optional wrapper
			dst: value.OptionalValue(value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
			err: nil,
		},
		// https://github.com/ydb-platform/ydb-go-sdk/issues/1515
		{
			name: xtest.CurrentFileLine(),
			src:  func() *uuid.UUID { return nil }(),
			dst:  value.NullValue(types.UUID),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  time.Unix(42, 43),
			dst:  value.TimestampValueFromTime(time.Unix(42, 43)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v time.Time) *time.Time { return &v }(time.Unix(42, 43)),
			dst:  value.OptionalValue(value.TimestampValueFromTime(time.Unix(42, 43))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *time.Time { return nil }(),
			dst:  value.NullValue(types.Timestamp),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  time.Duration(42),
			dst:  value.IntervalValueFromDuration(time.Duration(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func(v time.Duration) *time.Duration { return &v }(time.Duration(42)),
			dst:  value.OptionalValue(value.IntervalValueFromDuration(time.Duration(42))),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  func() *time.Duration { return nil }(),
			dst:  value.NullValue(types.Interval),
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
			dst: value.OptionalValue(value.StructValue(
				value.StructValueField{
					Name: "a",
					V:    value.TextValue("a"),
				},
				value.StructValueField{
					Name: "b",
					V:    value.Uint64Value(123),
				},
				value.StructValueField{
					Name: "c",
					V: value.ListValue(
						value.TextValue("1"),
						value.TextValue("2"),
						value.TextValue("3"),
					),
				},
			)),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: struct {
				A struct {
					Unsupported string
				} `sql:"A"`
			}{},
			dst: nil,
			err: errUnsupportedType,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  []uint64{123, 123, 123, 123, 123, 123},
			dst: value.ListValue(
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
			),
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src: []value.Value{
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
			},
			dst: value.ListValue(
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
				value.Uint64Value(123),
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
			dst: value.StructValue(
				value.StructValueField{
					Name: "a",
					V:    value.TextValue("a"),
				},
				value.StructValueField{
					Name: "b",
					V:    value.Uint64Value(123),
				},
				value.StructValueField{
					Name: "c",
					V:    value.ListValue(value.TextValue("1"), value.TextValue("2"), value.TextValue("3")),
				},
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
			dst: value.OptionalValue(value.StructValue(
				value.StructValueField{
					Name: "a",
					V:    value.TextValue("a"),
				},
				value.StructValueField{
					Name: "b",
					V:    value.Uint64Value(123),
				},
				value.StructValueField{
					Name: "c",
					V:    value.ListValue(value.TextValue("1"), value.TextValue("2"), value.TextValue("3")),
				},
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
			dst: value.NullValue(types.NewStruct(
				types.StructField{
					Name: "a",
					T:    types.Text,
				},
				types.StructField{
					Name: "b",
					T:    types.Uint64,
				},
				types.StructField{
					Name: "c",
					T:    types.NewList(types.Text),
				},
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
			dst: value.DictValue(
				value.DictValueField{
					K: value.Uint64Value(1),
					V: value.TextValue("1"),
				},
				value.DictValueField{
					K: value.Uint64Value(2),
					V: value.Uint64Value(2),
				},
				value.DictValueField{
					K: value.Uint64Value(3),
					V: value.ListValue(
						value.OptionalValue(value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
						value.OptionalValue(value.Uuid(uuid.UUID{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})),
						value.OptionalValue(value.Uuid(uuid.UUID{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})),
					),
				},
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
			dst: value.OptionalValue(value.DictValue(
				value.DictValueField{
					K: value.Uint64Value(1),
					V: value.TextValue("1"),
				},
				value.DictValueField{
					K: value.Uint64Value(2),
					V: value.Uint64Value(2),
				},
				value.DictValueField{
					K: value.Uint64Value(3),
					V: value.ListValue(
						value.OptionalValue(value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})),
						value.OptionalValue(value.Uuid(uuid.UUID{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})),
						value.OptionalValue(value.Uuid(uuid.UUID{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})),
					),
				},
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
		{
			name: xtest.CurrentFileLine(),
			src:  testValuer{value: "1234567890"},
			dst:  value.TextValue("1234567890"),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  testValuer{value: uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}},
			dst:  value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  testValuer{value: func() *string { return nil }()},
			dst:  value.NullValue(types.Text),
			err:  nil,
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

func named(name string, value any) driver.NamedValue {
	return driver.NamedValue{
		Name:  name,
		Value: value,
	}
}

func TestYdbParam(t *testing.T) {
	for _, tt := range []struct {
		name string
		src  any
		dst  *params.Parameter
		err  error
	}{
		{
			name: xtest.CurrentFileLine(),
			src:  params.Named("$a", value.Int32Value(42)),
			dst:  params.Named("$a", value.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("a", int(42)),
			dst:  params.Named("$a", value.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("$a", int(42)),
			dst:  params.Named("$a", value.Int32Value(42)),
			err:  nil,
		},
		{
			name: xtest.CurrentFileLine(),
			src:  named("a", uint(42)),
			dst:  params.Named("$a", value.Uint32Value(42)),
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
		args   []any
		params []*params.Parameter
		err    error
	}{
		{
			name:   xtest.CurrentFileLine(),
			args:   []any{},
			params: []*params.Parameter{},
			err:    nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				1, uint64(2), "3",
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				table.NewQueryParameters(
					params.Named("$p0", value.Int32Value(1)),
					params.Named("$p1", value.Uint64Value(2)),
					params.Named("$p2", value.TextValue("3")),
				),
				table.NewQueryParameters(
					params.Named("$p0", value.Int32Value(1)),
					params.Named("$p1", value.Uint64Value(2)),
					params.Named("$p2", value.TextValue("3")),
				),
			},
			err: errMultipleQueryParameters,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				sql.Named("$p0", value.Int32Value(1)),
				sql.Named("$p1", value.Uint64Value(2)),
				sql.Named("$p2", value.TextValue("3")),
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				driver.NamedValue{Name: "$p0", Value: value.Int32Value(1)},
				driver.NamedValue{Name: "$p1", Value: value.Uint64Value(2)},
				driver.NamedValue{Name: "$p2", Value: value.TextValue("3")},
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				driver.NamedValue{Value: params.Named("$p0", value.Int32Value(1))},
				driver.NamedValue{Value: params.Named("$p1", value.Uint64Value(2))},
				driver.NamedValue{Value: params.Named("$p2", value.TextValue("3"))},
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				driver.NamedValue{Value: 1},
				driver.NamedValue{Value: uint64(2)},
				driver.NamedValue{Value: "3"},
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				driver.NamedValue{Value: table.NewQueryParameters(
					params.Named("$p0", value.Int32Value(1)),
					params.Named("$p1", value.Uint64Value(2)),
					params.Named("$p2", value.TextValue("3")),
				)},
			},
			params: []*params.Parameter{
				params.Named("$p0", value.Int32Value(1)),
				params.Named("$p1", value.Uint64Value(2)),
				params.Named("$p2", value.TextValue("3")),
			},
			err: nil,
		},
		{
			name: xtest.CurrentFileLine(),
			args: []any{
				driver.NamedValue{Value: table.NewQueryParameters(
					params.Named("$p0", value.Int32Value(1)),
					params.Named("$p1", value.Uint64Value(2)),
					params.Named("$p2", value.TextValue("3")),
				)},
				driver.NamedValue{Value: params.Named("$p1", value.Uint64Value(2))},
				driver.NamedValue{Value: params.Named("$p2", value.TextValue("3"))},
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
			require.Equal(t, value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), v)
		})
		t.Run("*uuid.UUID", func(t *testing.T) {
			v, ok := asUUID(&uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			require.True(t, ok)
			require.Equal(t, value.OptionalValue(expUUIDValue), v)
		})
	})
	t.Run("Invalid", func(t *testing.T) {
		v, ok := asUUID([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		require.False(t, ok)
		require.Nil(t, v)
	})
}

func TestAsUUIDCastToInterface(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		t.Run("uuid.UUID", func(t *testing.T) {
			v, ok := asUUIDCastToInterface(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			require.True(t, ok)
			require.Equal(t, value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), v)
		})
		t.Run("*uuid.UUID", func(t *testing.T) {
			v, ok := asUUIDCastToInterface(&uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			require.True(t, ok)
			require.Equal(t, value.OptionalValue(value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})), v) //nolint:lll
		})
	})
	t.Run("Invalid", func(t *testing.T) {
		v, ok := asUUIDCastToInterface([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
		require.False(t, ok)
		require.Nil(t, v)
	})
}

func asUUIDCastToInterface(v any) (value.Value, bool) {
	// explicit casting of type [16]byte to uuid.UUID will success,
	// but casting of [16]byte to some interface with  methods from uuid.UUID will failed
	if _, ok := v.(interface {
		URN() string
	}); !ok {
		return nil, false
	}

	switch vv := v.(type) {
	case uuid.UUID:
		return value.Uuid(vv), true
	case *uuid.UUID:
		if vv == nil {
			return value.NullValue(types.UUID), true
		}

		return value.OptionalValue(value.Uuid(*vv)), true
	default:
		return nil, false
	}
}

func asUUIDForceTypeCast(v any) (value.Value, bool) {
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

func BenchmarkAsUUIDCastToInterface(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v, ok := asUUIDCastToInterface(srcUUID)
		require.True(b, ok)
		require.Equal(b, expUUIDValue, v)
	}
}

var (
	srcUUID      = uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	expUUIDValue = value.Uuid(uuid.UUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
)

func asUUIDUsingReflect(v any) (value.Value, bool) {
	switch reflect.TypeOf(v) {
	case uuidType:
		return value.Uuid(v.(uuid.UUID)), true
	case uuidPtrType:
		if v == nil {
			return value.NullValue(types.UUID), false
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
