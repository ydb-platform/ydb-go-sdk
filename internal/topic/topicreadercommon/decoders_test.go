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

var defaultDecodersCount = len(NewMultiDecoder().m)

func TestMultiDecoder(t *testing.T) {
	compressGzip := func(data string) io.Reader {
		buf := &bytes.Buffer{}

		gzipWriter := gzip.NewWriter(buf)
		_, err := gzipWriter.Write([]byte(data))
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		return buf
	}

	t.Run("NotResettableReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()
		require.Len(t, testMultiDecoder.m, defaultDecodersCount)

		buf := &bytes.Buffer{}
		_, err := buf.WriteString("test_data")
		require.NoError(t, err)

		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecRaw, buf)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data", string(decoded))

		creator := testMultiDecoder.m[rawtopiccommon.CodecRaw]
		require.Nil(t, creator.pool)
	})

	t.Run("ResettableReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		encodedReader := compressGzip("test_data_1")
		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedReader)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_1", string(decoded))

		encodedReader = compressGzip("test_data_2")
		decodedReader, err = testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedReader)
		require.NoError(t, err)

		decoded, err = io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_2", string(decoded))

		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]
		require.NotNil(t, creator.pool.Get())
	})

	t.Run("NotResettableReaderCustom", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		customCodec := rawtopiccommon.Codec(1001)
		testMultiDecoder.AddDecoder(customCodec, func(r io.Reader) (io.Reader, error) {
			rd, err := gzip.NewReader(r)
			if err != nil {
				return nil, err
			}

			return &notResettableReader{Reader: rd}, nil
		})
		require.Len(t, testMultiDecoder.m, defaultDecodersCount+1)

		encodedReader := compressGzip("test_data_1")
		decodedReader, err := testMultiDecoder.Decode(customCodec, encodedReader)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_1", string(decoded))

		creator := testMultiDecoder.m[customCodec]
		require.Nil(t, creator.pool.Get())
	})

	t.Run("ResettableReaderCustom", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		customCodec := rawtopiccommon.Codec(1001)
		testMultiDecoder.AddDecoder(customCodec, func(r io.Reader) (io.Reader, error) {
			return gzip.NewReader(r)
		})
		require.Len(t, testMultiDecoder.m, defaultDecodersCount+1)

		encodedReader := compressGzip("test_data_1")
		decodedReader, err := testMultiDecoder.Decode(customCodec, encodedReader)
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "test_data_1", string(decoded))

		creator := testMultiDecoder.m[customCodec]
		require.NotNil(t, creator.pool.Get())
	})

	t.Run("ResettableReaderManyMessages", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		for i := range 50 {
			testMsg := fmt.Sprintf("test_data_%d", i)
			encodedReader := compressGzip(testMsg)

			decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedReader)
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, testMsg, string(decoded))
		}

		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]
		require.NotNil(t, creator.pool.Get())
	})

	t.Run("CodecRawDoesNotPoolResettableInput", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		creator := testMultiDecoder.m[rawtopiccommon.CodecRaw]
		require.Nil(t, creator.pool, "raw codec must not own a pool")

		input1 := &resettableReaderTracker{Reader: bytes.NewBufferString("raw_payload_1")}
		decodedReader1, err := testMultiDecoder.Decode(rawtopiccommon.CodecRaw, input1)
		require.NoError(t, err)

		decoded1, err := io.ReadAll(decodedReader1)
		require.NoError(t, err)
		require.Equal(t, "raw_payload_1", string(decoded1))

		input2 := &resettableReaderTracker{Reader: bytes.NewBufferString("raw_payload_2")}
		decodedReader2, err := testMultiDecoder.Decode(rawtopiccommon.CodecRaw, input2)
		require.NoError(t, err)

		decoded2, err := io.ReadAll(decodedReader2)
		require.NoError(t, err)
		require.Equal(t, "raw_payload_2", string(decoded2))

		require.Zero(t, input1.resetCalls, "first caller's reader must not be Reset by decoder")
		require.Zero(t, input2.resetCalls, "second caller's reader must not be Reset by decoder")
	})

	t.Run("ReadAfterEOFDoesNotDoublePool", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, compressGzip("payload"))
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "payload", string(decoded))

		buf := make([]byte, 16)
		for range 3 {
			n, err := decodedReader.Read(buf)
			require.Zero(t, n)
			require.ErrorIs(t, err, io.EOF)
		}

		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]
		require.NotNil(t, creator.pool.Get(), "pool should contain the returned decoder")
		require.Nil(t, creator.pool.Get(), "pool must not contain a duplicate put")
	})

	t.Run("ReadAfterEOFDoesNotUsePooledReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		first, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, compressGzip("first"))
		require.NoError(t, err)

		_, err = io.ReadAll(first)
		require.NoError(t, err)

		second, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, compressGzip("second"))
		require.NoError(t, err)

		buf := make([]byte, 16)
		n, err := first.Read(buf)
		require.Zero(t, n)
		require.ErrorIs(t, err, io.EOF)

		decoded, err := io.ReadAll(second)
		require.NoError(t, err)
		require.Equal(t, "second", string(decoded))
	})
}

type notResettableReader struct {
	io.Reader
}

type resettableReaderTracker struct {
	io.Reader

	resetCalls int
}

func (r *resettableReaderTracker) Reset(rd io.Reader) error {
	r.resetCalls++
	r.Reader = rd

	return nil
}
