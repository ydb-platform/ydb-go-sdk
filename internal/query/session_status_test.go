package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatusString(t *testing.T) {
	tests := []struct {
		name     string
		status   Status
		expected string
	}{
		{
			name:     "StatusUnknown",
			status:   StatusUnknown,
			expected: "Unknown",
		},
		{
			name:     "StatusIdle",
			status:   StatusIdle,
			expected: "Idle",
		},
		{
			name:     "StatusInUse",
			status:   StatusInUse,
			expected: "InUse",
		},
		{
			name:     "StatusClosing",
			status:   StatusClosing,
			expected: "Closing",
		},
		{
			name:     "StatusClosed",
			status:   StatusClosed,
			expected: "Closed",
		},
		{
			name:     "StatusError",
			status:   StatusError,
			expected: "Error",
		},
		{
			name:     "InvalidStatus",
			status:   Status(999),
			expected: "Unknown999",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.status.String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsAlive(t *testing.T) {
	tests := []struct {
		name     string
		status   Status
		expected bool
	}{
		{
			name:     "StatusUnknown",
			status:   StatusUnknown,
			expected: true,
		},
		{
			name:     "StatusIdle",
			status:   StatusIdle,
			expected: true,
		},
		{
			name:     "StatusInUse",
			status:   StatusInUse,
			expected: true,
		},
		{
			name:     "StatusClosing",
			status:   StatusClosing,
			expected: false,
		},
		{
			name:     "StatusClosed",
			status:   StatusClosed,
			expected: false,
		},
		{
			name:     "StatusError",
			status:   StatusError,
			expected: false,
		},
		{
			name:     "CustomStatusValue",
			status:   Status(100),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAlive(tt.status)
			require.Equal(t, tt.expected, result)
		})
	}
}
