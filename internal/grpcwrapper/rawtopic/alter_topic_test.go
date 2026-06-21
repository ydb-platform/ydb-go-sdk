package rawtopic

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestAlterConsumerToProto(t *testing.T) {
	t.Run("OmitsSupportedCodecsWhenNotSet", func(t *testing.T) {
		// Altering an unrelated consumer property must not touch supported codecs,
		// otherwise the server resets the consumer's codec restriction to "allow any".
		c := &AlterConsumer{
			Name:         "consumer",
			SetImportant: rawoptional.Bool{Value: true, HasValue: true},
		}

		proto := c.ToProto()
		require.Nil(t, proto.GetSetSupportedCodecs())
	})

	t.Run("SetsSupportedCodecsWhenRequested", func(t *testing.T) {
		c := &AlterConsumer{
			Name:                   "consumer",
			NeedSetSupportedCodecs: true,
			SetSupportedCodecs:     rawtopiccommon.SupportedCodecs{rawtopiccommon.CodecGzip},
		}

		proto := c.ToProto()
		require.NotNil(t, proto.GetSetSupportedCodecs())
		require.Equal(t, []int32{int32(rawtopiccommon.CodecGzip)}, proto.GetSetSupportedCodecs().GetCodecs())
	})

	t.Run("SetsEmptySupportedCodecsWhenRequestedWithEmptyList", func(t *testing.T) {
		c := &AlterConsumer{
			Name:                   "consumer",
			NeedSetSupportedCodecs: true,
		}

		proto := c.ToProto()
		require.NotNil(t, proto.GetSetSupportedCodecs())
		require.Empty(t, proto.GetSetSupportedCodecs().GetCodecs())
	})
}
