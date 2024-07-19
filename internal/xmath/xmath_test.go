package xmath

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMin(t *testing.T) {
	require.Equal(t, 1, Min(1, 2, 3))
}

func TestMax(t *testing.T) {
	require.Equal(t, 3, Max(1, 2, 3))
}
