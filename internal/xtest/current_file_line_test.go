package xtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCurrentFileLine(t *testing.T) {
	require.Equal(t, "current_file_line_test.go:10", CurrentFileLine())
}
