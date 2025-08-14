package xstring

import (
	"bytes"
	"testing"
)

func BenchmarkBufferWithPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		func() {
			buffer := Buffer()
			buffer.WriteString(b.Name())
			defer buffer.Free()
		}()
	}
}

func BenchmarkBufferWithoutPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		func() {
			buffer := bytes.Buffer{}
			buffer.WriteString(b.Name())
		}()
	}
}
