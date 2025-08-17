package xtest

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallMethod(t *testing.T) {
	object := bytes.NewBuffer(nil)

	result := CallMethod(object, "WriteString", "Hello world!")
	n := result[0].(int)
	err := result[1]

	require.Equal(t, 12, n)
	require.Nil(t, err)

	result = CallMethod(object, "String")

	str, ok := result[0].(string)
	require.True(t, ok)

	require.Equal(t, object.String(), str)

	require.Panics(t, func() {
		CallMethod(object, "NonameMethod")
	})

	require.Panics(t, func() {
		CallMethod(object, "String", "wrong", "arguments", "count")
	})

	require.Panics(t, func() {
		// Wrong argument type.
		CallMethod(object, "WriteString", 123)
	})
}
