package xreflect

import (
	"testing"
)

func TestIsContainsNilPointer(t *testing.T) {
	var nilIntPointer *int
	var vInterface any
	vInterface = nilIntPointer

	// Test cases for different nil and non-nil scenarios
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{
			name:     "nil interface",
			input:    nil,
			expected: true,
		},
		{
			name:     "nil pointer to int",
			input:    (*int)(nil),
			expected: true,
		},
		{
			name:     "non-nil pointer to int",
			input:    new(int),
			expected: false,
		},
		{
			name:     "nil slice",
			input:    []int(nil),
			expected: false,
		},
		{
			name:     "empty slice",
			input:    []int{},
			expected: false,
		},
		{
			name:     "nil map",
			input:    map[string]int(nil),
			expected: true,
		},
		{
			name:     "empty map",
			input:    map[string]int{},
			expected: false,
		},
		{
			name:     "nil channel",
			input:    (chan int)(nil),
			expected: true,
		},
		{
			name:     "non-nil channel",
			input:    make(chan int),
			expected: false,
		},
		{
			name:     "nil function",
			input:    (func())(nil),
			expected: true,
		},
		{
			name:     "nested nil pointer",
			input:    &nilIntPointer,
			expected: true,
		},
		{
			name:     "interface with stored nil pointer",
			input:    vInterface,
			expected: true,
		},
		{
			name:     "non-nil interface value",
			input:    interface{}("test"),
			expected: false,
		},
	}

	// Execute all test cases
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsContainsNilPointer(tt.input); got != tt.expected {
				t.Errorf("IsContainsNilPointer() = %v, want %v", got, tt.expected)
			}
		})
	}
}
