package conn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeErrorError(t *testing.T) {
	testErr := errors.New("test")
	nodeErr := newConnError(1, "localhost:1234", testErr)
	message := nodeErr.Error()

	require.Equal(t, "connError{node_id:1,address:'localhost:1234'}: test", message)
}

func TestNodeErrorUnwrap(t *testing.T) {
	testErr := errors.New("test")
	nodeErr := newConnError(1, "asd", testErr)

	unwrapped := errors.Unwrap(nodeErr)
	require.Equal(t, testErr, unwrapped)
}

func TestNodeErrorIs(t *testing.T) {
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	nodeErr := newConnError(1, "localhost:1234", testErr)

	require.ErrorIs(t, nodeErr, testErr)
	require.NotErrorIs(t, nodeErr, testErr2)
}

type testType1Error struct {
	msg string
}

func (t testType1Error) Error() string {
	return "1 - " + t.msg
}

type testType2Error struct {
	msg string
}

func (t testType2Error) Error() string {
	return "2 - " + t.msg
}

func TestNodeErrorAs(t *testing.T) {
	testErr := testType1Error{msg: "test"}
	nodeErr := newConnError(1, "localhost:1234", testErr)

	target := testType1Error{}
	require.ErrorAs(t, nodeErr, &target)
	require.Equal(t, testErr, target)

	target2 := testType2Error{}
	require.False(t, errors.As(nodeErr, &target2))
}
