package topicreadercommon

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestMultiDecoder(t *testing.T) {
	compressGzip := func(data string) []byte {
		buf := &bytes.Buffer{}

		gzipWriter := gzip.NewWriter(buf)
		_, err := gzipWriter.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		return buf.Bytes()
	}

	t.Run("NotResettableReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()
		require.Len(t, testMultiDecoder.m, 2)
		require.Len(t, testMultiDecoder.dp, 2)

		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecRaw, []byte("test_data"))
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data", string(decoded))
	})

	t.Run("ResettableReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		encodedData := compressGzip("test_data_1")
		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedData)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_1", string(decoded))

		encodedData = compressGzip("test_data_2")
		decodedReader, err = testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedData)
		require.NoError(t, err)

		decoded, err = io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_2", string(decoded))
	})

	t.Run("ResettableReaderCustom", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		customCodec := rawtopiccommon.Codec(1001)
		testMultiDecoder.AddDecoder(customCodec, func(r io.Reader) (io.Reader, error) {
			return gzip.NewReader(r)
		})
		require.Len(t, testMultiDecoder.m, 3)
		require.Len(t, testMultiDecoder.dp, 3)

		encodedData := compressGzip("test_data_1")
		decodedReader, err := testMultiDecoder.Decode(customCodec, encodedData)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_1", string(decoded))
	})

	t.Run("ResettableReaderManyMessages", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		for i := 0; i < 50; i++ {
			testMsg := fmt.Sprintf("test_data_%d", i)
			encodedData := compressGzip(testMsg)

			decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedData)
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, testMsg, string(decoded))
		}
	})
}
