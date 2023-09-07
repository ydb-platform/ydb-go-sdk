package conn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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

func TestNodeErrorIs(t *testing.T) {
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	nodeErr := newNodeError(1, "localhost:1234", testErr)

	require.True(t, errors.Is(nodeErr, testErr))
	require.False(t, errors.Is(nodeErr, testErr2))
}

type testErrorType1 struct {
	msg string
}

func (t testErrorType1) Error() string {
	return "1 - " + t.msg
}

type testErrorType2 struct {
	msg string
}

func (t testErrorType2) Error() string {
	return "2 - " + t.msg
}

func TestNodeErrorAs(t *testing.T) {
	testErr := testErrorType1{msg: "test"}
	nodeErr := newNodeError(1, "localhost:1234", testErr)

	target := testErrorType1{}
	require.True(t, errors.As(nodeErr, &target))
	require.ErrorAs(t, nodeErr, &target)
	require.Equal(t, testErr, target)

	target2 := testErrorType2{}
	require.False(t, errors.As(nodeErr, &target2))
}
