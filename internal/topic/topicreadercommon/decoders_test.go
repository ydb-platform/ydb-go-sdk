package topicreadercommon

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestDecoderMap(t *testing.T) {
	decoderMap := NewDecoderMap()

	t.Run("DecodeRaw", func(t *testing.T) {
		data := []byte("test data")
		reader := bytes.NewReader(data)

		decodedReader, err := decoderMap.Decode(rawtopiccommon.CodecRaw, reader)
		require.NoError(t, err)

		result, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("DecodeGzip", func(t *testing.T) {
		data := []byte("test data")
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		decodedReader, err := decoderMap.Decode(rawtopiccommon.CodecGzip, &buf)
		require.NoError(t, err)

		result, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, data, result)
	})

	t.Run("DecodeUnknownCodec", func(t *testing.T) {
		_, err := decoderMap.Decode(rawtopiccommon.Codec(999), bytes.NewReader([]byte{}))
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrPublicUnexpectedCodec))
	})
}
