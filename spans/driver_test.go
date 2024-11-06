package spans

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkTraceparent(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		require.Equal(b,
			"00-8e3790822789a6917883e08d0eeb783e-729d847ca290963e-00",
			traceparent("8e3790822789a6917883e08d0eeb783e", "729d847ca290963e"),
		)
	}
}
