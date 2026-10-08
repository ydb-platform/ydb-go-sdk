package value

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
)

func BenchmarkOptionalValue(b *testing.B) {
	b.Run("Construct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkOptionalValue = OptionalValue(Int32Value(42))
		}
	})
	b.Run("Type", func(b *testing.B) {
		v := OptionalValue(Int32Value(42))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchmarkOptionalType = v.Type()
		}
	})
}

var (
	benchmarkOptionalValue Value
	benchmarkOptionalType  types.Type
)
