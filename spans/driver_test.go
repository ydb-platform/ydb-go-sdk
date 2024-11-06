package spans

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

func traceparentFmt(traceID string, spanID string) string {
	return fmt.Sprintf("00-%s-%s-0", traceID, spanID)
}

func BenchmarkTraceparentFmt(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		traceparent := traceparentFmt("8e3790822789a6917883e08d0eeb783e", "729d847ca290963e")
		require.Equal(b, "00-8e3790822789a6917883e08d0eeb783e-729d847ca290963e-0", traceparent)
	}
}

func traceparentBuilder(traceID string, spanID string) string {
	var b strings.Builder
	b.WriteString("00-")
	b.WriteString(traceID)
	b.WriteByte('-')
	b.WriteString(spanID)
	b.WriteString("-0")

	return b.String()
}

func BenchmarkTraceparentBuilder(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		traceparent := traceparentBuilder("8e3790822789a6917883e08d0eeb783e", "729d847ca290963e")
		require.Equal(b, "00-8e3790822789a6917883e08d0eeb783e-729d847ca290963e-0", traceparent)
	}
}

func traceparentBuffer(traceID string, spanID string) string {
	b := xstring.Buffer()
	defer b.Free()

	b.WriteString("00-")
	b.WriteString(traceID)
	b.WriteByte('-')
	b.WriteString(spanID)
	b.WriteString("-0")

	return b.String()
}

func BenchmarkTraceparentBuffer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		traceparent := traceparentBuffer("8e3790822789a6917883e08d0eeb783e", "729d847ca290963e")
		require.Equal(b, "00-8e3790822789a6917883e08d0eeb783e-729d847ca290963e-0", traceparent)
	}
}

func BenchmarkTraceparent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		traceparent := traceparent("8e3790822789a6917883e08d0eeb783e", "729d847ca290963e")
		require.Equal(b, "00-8e3790822789a6917883e08d0eeb783e-729d847ca290963e-0", traceparent)
	}
}
