//nolint:goconst
package xstring

import (
	"testing"
)

// Test the performance of the standard conversion string()
func Benchmark_NormalFromBytes(b *testing.B) {
	x := []byte("Hello world! Hello world! Hello world!")
	for i := 0; i < b.N; i++ {
		_ = string(x)
	}
}

// Test the performance of strong conversion []byte to string
func Benchmark_FromBytes(b *testing.B) {
	x := []byte("Hello world! Hello world! Hello world!")
	for i := 0; i < b.N; i++ {
		_ = FromBytes(x)
	}
}

// Test the performance of standard conversion []byte
func Benchmark_NormalToBytes(b *testing.B) {
	x := "Hello world! Hello world! Hello world!"
	for i := 0; i < b.N; i++ {
		_ = []byte(x)
	}
}

// Test the performance of strong conversion string to []byte
func Benchmark_ToBytes(b *testing.B) {
	x := "Hello world! Hello world! Hello world!"
	for i := 0; i < b.N; i++ {
		_ = ToBytes(x)
	}
}
