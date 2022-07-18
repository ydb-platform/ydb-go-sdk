package types

import "testing"

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
			TypeUTF8,
			false,
		},
		{
			TypeUTF8,
			TypeUTF8,
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeBool),
			true,
		},
		{
			Optional(TypeBool),
			Optional(TypeUTF8),
			false,
		},
		{
			Optional(TypeUTF8),
			Optional(TypeUTF8),
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
