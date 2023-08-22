package conn

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeErrorError(t *testing.T) {
	testErr := errors.New("test")
	nodeErr := newNodeError(1, "localhost:1234", testErr)
	message := nodeErr.Error()

	require.Equal(t, "on node 1 (localhost:1234): test", message)
}

func TestNodeErrorUnwrap(t *testing.T) {
	testErr := errors.New("test")
	nodeErr := newNodeError(1, "asd", testErr)

	unwrapped := errors.Unwrap(nodeErr)
	require.Equal(t, testErr, unwrapped)
}
