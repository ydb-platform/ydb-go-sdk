package topicwriterinternal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzipHeaderSize(t *testing.T) {
	require.NotEmpty(t, gzipHeaderSize)
}
