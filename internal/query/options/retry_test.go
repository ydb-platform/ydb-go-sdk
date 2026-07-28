package options

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithIdempotent(t *testing.T) {
	require.Len(t, WithIdempotent(), 1)
	require.Len(t, WithIdempotent(false), 1)
	require.Panics(t, func() {
		WithIdempotent(true, false)
	})
}
