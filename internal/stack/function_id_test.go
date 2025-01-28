package stack

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type genericType[T any] struct{}

type starType struct{}

func (t genericType[T]) Call() string {
	return FunctionID("").String()
}

func staticCall() string {
	return FunctionID("").String()
}

func (e *starType) starredCall() string {
	return FunctionID("").String()
}

func anonymousFunctionCall() string {
	var result string
	var mu sync.Mutex
	go func() {
		mu.Lock()
		defer mu.Unlock()
		result = FunctionID("").String()
	}()
	time.Sleep(time.Second)

	mu.Lock()
	defer mu.Unlock()

	return result
}

func callWithPackageDefinition() string {
	return FunctionID("", Package("database/sql")).String()
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
	t.Run("StarTypeCall", func(t *testing.T) {
		x := starType{}
		require.Equal(t,
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.(*starType).starredCall",
			x.starredCall(),
		)
	})
	t.Run("AnonymousFunctionCall", func(t *testing.T) {
		require.Equal(t,
			"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack.anonymousFunctionCall",
			anonymousFunctionCall(),
		)
	})
	t.Run("CallWithPackageDefinition", func(t *testing.T) {
		require.Equal(t,
			"database/sql.callWithPackageDefinition",
			callWithPackageDefinition(),
		)
	})
}
