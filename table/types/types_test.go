package types

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
)

func TestEqual(t *testing.T) {
	tests := []struct {
		lhs   Type
		rhs   Type
		equal bool
	}{
		{
			TypeBool,
			TypeBool,
			true,
		},
		{
			TypeBool,
			TypeText,
			false,
		},
		{
			TypeText,
			TypeText,
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeBool),
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeText),
			false,
		},
		{
			Optional(TypeText),
			Optional(TypeText),
			true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			if equal := Equal(tt.lhs, tt.rhs); equal != tt.equal {
				t.Errorf("Equal(%s, %s) = %v, want %v", tt.lhs, tt.rhs, equal, tt.equal)
			}
		})
	}
}

func TestOptionalInnerType(t *testing.T) {
	tests := []struct {
		src        Type
		innerType  Type
		isOptional bool
	}{
		{
			TypeBool,
			nil,
			false,
		},
		{
			TypeText,
			nil,
			false,
		},
		{
			Optional(TypeBool),
			TypeBool,
			true,
		},
		{
			Optional(TypeText),
			TypeText,
			true,
		},
		{
			Optional(Tuple(TypeText, TypeBool, TypeUint64, Optional(TypeInt64))),
			Tuple(TypeText, TypeBool, TypeUint64, Optional(TypeInt64)),
			true,
		},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			optional, isOptional := tt.src.(interface {
				IsOptional()
				InnerType() Type
			})
			require.Equal(t, tt.isOptional, isOptional)
			var innerType Type
			if isOptional {
				innerType = optional.InnerType()
			}
			if tt.innerType == nil {
				require.Nil(t, innerType)
			} else {
				require.True(t, Equal(tt.innerType, innerType))
			}
		})
	}
}

func TestMakeValueByType(t *testing.T) {
	for _, tt := range []struct {
		t     Type
		v     interface{}
		value Value
		errIs error
		errAs error
	}{
		{
			t:     TypeBool,
			v:     "true",
			value: BoolValue(true),
			errIs: nil,
		},
		{
			t:     TypeBool,
			v:     "1",
			value: BoolValue(true),
			errIs: nil,
		},
		{
			t:     TypeBool,
			v:     "unknown",
			value: nil,
			errAs: &strconv.NumError{},
		},
		{
			t:     Optional(TypeBool),
			v:     func(s string) *string { return &s }("unknown"),
			value: nil,
			errAs: &strconv.NumError{},
		},
		{
			t:     TypeBool,
			v:     true,
			value: BoolValue(true),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     true,
			value: OptionalValue(BoolValue(true)),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     func(b bool) *bool { return &b }(true),
			value: OptionalValue(BoolValue(true)),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     func() *bool { return nil }(),
			value: NullValue(TypeBool),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     nil,
			value: NullValue(TypeBool),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     OptionalValue(BoolValue(true)),
			value: OptionalValue(BoolValue(true)),
			errIs: nil,
		},
		{
			t:     Optional(TypeBool),
			v:     BoolValue(true),
			value: nil,
			errIs: ErrInconsistentTypes,
		},
		{
			t:     TypeUint32,
			v:     123,
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     uint(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int32(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int32(-123),
			value: nil,
			errIs: value.ErrPrimitiveTypeNotSupportCastFromInterface,
		},
		{
			t:     TypeUint32,
			v:     uint32(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int8(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int8(-123),
			value: nil,
			errIs: value.ErrPrimitiveTypeNotSupportCastFromInterface,
		},
		{
			t:     TypeUint32,
			v:     uint8(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int16(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int16(-123),
			value: nil,
			errIs: value.ErrPrimitiveTypeNotSupportCastFromInterface,
		},
		{
			t:     TypeUint32,
			v:     uint16(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int64(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     TypeUint32,
			v:     int64(-123),
			value: nil,
			errIs: value.ErrPrimitiveTypeNotSupportCastFromInterface,
		},
		{
			t:     TypeUint32,
			v:     uint64(123),
			value: Uint32Value(123),
			errIs: nil,
		},
		{
			t:     Optional(TypeUint32),
			v:     123,
			value: OptionalValue(Uint32Value(123)),
			errIs: nil,
		},
		{
			t:     Optional(TypeUint32),
			v:     func(v int) *int { return &v }(123),
			value: OptionalValue(Uint32Value(123)),
			errIs: nil,
		},
		{
			t:     Optional(TypeUint32),
			v:     nil,
			value: NullValue(TypeUint32),
			errIs: nil,
		},
		{
			t:     Optional(DecimalType(10, 20)),
			v:     nil,
			value: NullValue(DecimalType(10, 20)),
			errIs: nil,
		},
		{
			t:     Optional(DecimalType(10, 20)),
			v:     DecimalType(10, 20),
			value: nil,
			errIs: value.ErrOptionalTypeNotSupportCastFromInterface,
		},
		{
			t:     List(TypeText),
			v:     []string{"1", "2", "3"},
			value: nil,
			errIs: ErrTypeNotSupportCastToValue,
		},
	} {
		t.Run(fmt.Sprintf("%s.(%s)=>%s", func() string {
			if tt.v == nil {
				return "<nil>"
			}
			if s, has := tt.v.(string); has {
				return fmt.Sprintf("%q", s)
			}
			if s, has := tt.v.(*string); has {
				return fmt.Sprintf("(&%q)", *s)
			}
			return fmt.Sprintf("%T(%v)", tt.v, tt.v)
		}(), tt.t.Yql(), func() string {
			if tt.errIs != nil {
				return tt.errIs.Error()
			}
			if tt.errAs != nil {
				return fmt.Sprintf("%T", tt.errAs)
			}
			return tt.value.Yql()
		}()), func(t *testing.T) {
			value, err := MakeValueByType(tt.t, tt.v)
			if tt.errIs != nil {
				require.ErrorIs(t, err, tt.errIs)
			} else if tt.errAs != nil {
				require.ErrorAs(t, err, &tt.errAs)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.value, value)
			}
		})
	}
}
