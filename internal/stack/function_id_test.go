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
	return RuntimeFunctionID("").String()
}

func staticCall() string {
	return RuntimeFunctionID("").String()
}

func (e *starType) starredCall() string {
	return RuntimeFunctionID("").String()
}

func anonymousFunctionCall() string {
	var result string
	var mu sync.Mutex
	go func() {
		mu.Lock()
		defer mu.Unlock()
		result = RuntimeFunctionID("").String()
	}()
	time.Sleep(time.Second)

	mu.Lock()
	defer mu.Unlock()

	return result
}

func callWithPackageDefinition() string {
	return RuntimeFunctionID("", Package("database/sql")).String()
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

// BenchmarkFunctionID/stack.FunctionID.literal-12		0.2677 ns/op	0 B/op	0 allocs/op
// BenchmarkFunctionID/stack.RuntimeFunctionID-12		11.51 ns/op	16 B/op	1 allocs/op
func BenchmarkFunctionID(b *testing.B) {
	b.Run("stack.FunctionID.literal", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = FunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query")
		}
	})
	b.Run("stack.RuntimeFunctionID", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = RuntimeFunctionID("github.com/ydb-platform/ydb-go-sdk/v3/internal/query.(*Session).Query")
		}
	})
}
