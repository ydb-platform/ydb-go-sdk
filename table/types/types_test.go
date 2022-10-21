package types

import (
	"testing"
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
