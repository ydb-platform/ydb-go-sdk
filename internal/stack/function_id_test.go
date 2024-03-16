package stack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type genericType[T any] struct{}

func (t genericType[T]) Call() string {
	return FunctionID("").FunctionID()
}

func staticCall() string {
	return FunctionID("").FunctionID()
}

func TestFunctionIDForGenericType(t *testing.T) {
	t.Run("StaticFunc", func(t *testing.T) {
		require.Equal(t,
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.staticCall",
			staticCall(),
		)
	})
	t.Run("GenericTypeCall", func(t *testing.T) {
		require.Equal(t,
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.genericType.Call",
			genericType[uint64]{}.Call(),
		)
	})
}
