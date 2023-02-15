package types

import (
	"testing"

	"github.com/stretchr/testify/require"
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
