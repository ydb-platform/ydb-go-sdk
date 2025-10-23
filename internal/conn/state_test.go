package conn

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		name     string
		state    State
		expected string
	}{
		{
			name:     "Created",
			state:    Created,
			expected: "created",
		},
		{
			name:     "Online",
			state:    Online,
			expected: "online",
		},
		{
			name:     "Banned",
			state:    Banned,
			expected: "banned",
		},
		{
			name:     "Offline",
			state:    Offline,
			expected: "offline",
		},
		{
			name:     "Destroyed",
			state:    Destroyed,
			expected: "destroyed",
		},
		{
			name:     "Unknown",
			state:    Unknown,
			expected: "unknown",
		},
		{
			name:     "Invalid value",
			state:    State(99),
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.state.String())
		})
	}
}

func TestState_Code(t *testing.T) {
	tests := []struct {
		name     string
		state    State
		expected int
	}{
		{
			name:     "Unknown",
			state:    Unknown,
			expected: 0,
		},
		{
			name:     "Created",
			state:    Created,
			expected: 1,
		},
		{
			name:     "Online",
			state:    Online,
			expected: 2,
		},
		{
			name:     "Banned",
			state:    Banned,
			expected: 3,
		},
		{
			name:     "Offline",
			state:    Offline,
			expected: 4,
		},
		{
			name:     "Destroyed",
			state:    Destroyed,
			expected: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.state.Code())
		})
	}
}

func TestState_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		state    State
		expected bool
	}{
		{
			name:     "Online is valid",
			state:    Online,
			expected: true,
		},
		{
			name:     "Offline is valid",
			state:    Offline,
			expected: true,
		},
		{
			name:     "Banned is valid",
			state:    Banned,
			expected: true,
		},
		{
			name:     "Unknown is not valid",
			state:    Unknown,
			expected: false,
		},
		{
			name:     "Created is not valid",
			state:    Created,
			expected: false,
		},
		{
			name:     "Destroyed is not valid",
			state:    Destroyed,
			expected: false,
		},
		{
			name:     "Invalid value is not valid",
			state:    State(99),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.state.IsValid())
		})
	}
}
