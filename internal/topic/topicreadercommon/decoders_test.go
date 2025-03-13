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

	t.Run("DecodeCustomCodec", func(t *testing.T) {
		dm := NewDecoderMap()
		customCodec := rawtopiccommon.Codec(1001)
		dm.AddDecoder(customCodec, func(input io.Reader) (ReadResetter, error) {
			return gzip.NewReader(input)
		})
		require.Len(t, dm.dp, 3)

		data := []byte("custom test data")
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		decodedReader, err := dm.Decode(customCodec, &buf)
		require.NoError(t, err)
		defer decodedReader.(io.Closer).Close()

		result, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, string(data), string(result))

		data2 := []byte("second test data")
		var buf2 bytes.Buffer
		gzipWriter2 := gzip.NewWriter(&buf2)
		_, err = gzipWriter2.Write(data2)
		require.NoError(t, err)
		require.NoError(t, gzipWriter2.Close())

		decodedReader2, err := dm.Decode(customCodec, &buf2)
		require.NoError(t, err)
		defer decodedReader2.(io.Closer).Close()

		result2, err := io.ReadAll(decodedReader2)
		require.NoError(t, err)
		require.Equal(t, string(data2), string(result2))
	})

	t.Run("DecodeCustomCodec", func(t *testing.T) {
		dm := NewDecoderMap()
		customCodec := rawtopiccommon.Codec(1001)
		dm.AddDecoder(customCodec, func(input io.Reader) (ReadResetter, error) {
			return gzip.NewReader(input)
		})
		require.Len(t, dm.dp, 3)

		data := []byte("custom test data")
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		decodedReader, err := dm.Decode(customCodec, &buf)
		require.NoError(t, err)
		result, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, string(data), string(result))
		require.NoError(t, decodedReader.(io.Closer).Close()) // Явный вызов Close вместо defer

		data2 := []byte("second test data")
		var buf2 bytes.Buffer
		gzipWriter2 := gzip.NewWriter(&buf2)
		_, err = gzipWriter2.Write(data2)
		require.NoError(t, err)
		require.NoError(t, gzipWriter2.Close())

		decodedReader2, err := dm.Decode(customCodec, &buf2)
		require.NoError(t, err)
		result2, err := io.ReadAll(decodedReader2)
		require.NoError(t, err)
		require.Equal(t, string(data2), string(result2))
		require.NoError(t, decodedReader2.(io.Closer).Close()) // Явный вызов Close вместо defer
	})

	t.Run("PoolReuse", func(t *testing.T) {
		dm := NewDecoderMap()
		customCodec := rawtopiccommon.Codec(1002)

		dm.AddDecoder(customCodec, func(input io.Reader) (ReadResetter, error) {
			return gzip.NewReader(input)
		})

		data1 := []byte("hello")
		var buf1 bytes.Buffer
		gzipWriter1 := gzip.NewWriter(&buf1)
		_, err := gzipWriter1.Write(data1)
		require.NoError(t, err)
		require.NoError(t, gzipWriter1.Close())

		reader1, err := dm.Decode(customCodec, &buf1)
		require.NoError(t, err, "first decoding should succeed")
		result1, err := io.ReadAll(reader1)
		require.NoError(t, err, "reading first message should succeed")
		require.Equal(t, string(data1), string(result1), "data should match")
		require.NoError(t, reader1.(io.Closer).Close(), "closing first reader should succeed")

		pool := dm.dp[customCodec]
		reusedDecoder := pool.Get()
		require.NotNil(t, reusedDecoder, "decoder should be returned to pool after Close")

		data2 := []byte("world")
		var buf2 bytes.Buffer
		gzipWriter2 := gzip.NewWriter(&buf2)
		_, err = gzipWriter2.Write(data2)
		require.NoError(t, err)
		require.NoError(t, gzipWriter2.Close())

		reader2, err := dm.Decode(customCodec, &buf2)
		require.NoError(t, err, "second decoding should succeed")
		result2, err := io.ReadAll(reader2)
		require.NoError(t, err, "reading second message should succeed")
		require.Equal(t, string(data2), string(result2), "data of second message should match")
		require.NoError(t, reader2.(io.Closer).Close(), "closing second reader should succeed")
	})
}
