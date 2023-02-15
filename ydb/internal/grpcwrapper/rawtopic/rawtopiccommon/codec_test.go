package rawtopiccommon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSupportedCodecsEquals(t *testing.T) {
	tests := []struct {
		first, second SupportedCodecs
		result        bool
	}{
		{
			nil,
			nil,
			true,
		},
		{
			nil,
			SupportedCodecs{},
			true,
		},
		{
			SupportedCodecs{CodecGzip, CodecRaw},
			SupportedCodecs{CodecGzip, CodecRaw},
			true,
		},
		{
			SupportedCodecs{CodecRaw, CodecGzip},
			SupportedCodecs{CodecGzip, CodecRaw},
			true,
		},
		{
			nil,
			SupportedCodecs{CodecRaw},
			false,
		},
		{
			SupportedCodecs{CodecGzip},
			SupportedCodecs{CodecRaw},
			false,
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			require.Equal(t, test.result, test.first.IsEqualsTo(test.second))
			require.Equal(t, test.result, test.second.IsEqualsTo(test.first))
		})
	}
}
