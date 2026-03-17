package conn

import (
	"testing"

	"github.com/stretchr/testify/require"

	state2 "github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		name     string
		state    state2.State
		expected string
	}{
		{
			name:     "Created",
			state:    state2.Created,
			expected: "created",
		},
		{
			name:     "Online",
			state:    state2.Online,
			expected: "online",
		},
		{
			name:     "Banned",
			state:    state2.Banned,
			expected: "banned",
		},
		{
			name:     "Offline",
			state:    state2.Offline,
			expected: "offline",
		},
		{
			name:     "Destroyed",
			state:    state2.Destroyed,
			expected: "destroyed",
		},
		{
			name:     "Unknown",
			state:    state2.Unknown,
			expected: "unknown",
		},
		{
			name:     "Invalid value",
			state:    state2.State(99),
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
		state    state2.State
		expected int
	}{
		{
			name:     "Unknown",
			state:    state2.Unknown,
			expected: 0,
		},
		{
			name:     "Created",
			state:    state2.Created,
			expected: 1,
		},
		{
			name:     "Online",
			state:    state2.Online,
			expected: 2,
		},
		{
			name:     "Banned",
			state:    state2.Banned,
			expected: 3,
		},
		{
			name:     "Offline",
			state:    state2.Offline,
			expected: 4,
		},
		{
			name:     "Destroyed",
			state:    state2.Destroyed,
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
		state    state2.State
		expected bool
	}{
		{
			name:     "Online is valid",
			state:    state2.Online,
			expected: true,
		},
		{
			name:     "Offline is valid",
			state:    state2.Offline,
			expected: true,
		},
		{
			name:     "Banned is valid",
			state:    state2.Banned,
			expected: true,
		},
		{
			name:     "Unknown is not valid",
			state:    state2.Unknown,
			expected: false,
		},
		{
			name:     "Created is not valid",
			state:    state2.Created,
			expected: false,
		},
		{
			name:     "Destroyed is not valid",
			state:    state2.Destroyed,
			expected: false,
		},
		{
			name:     "Invalid value is not valid",
			state:    state2.State(99),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.state.IsValid())
		})
	}
}
