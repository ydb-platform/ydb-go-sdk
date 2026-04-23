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

// Number of iterations to ensure pooled decoder reuse
// despite sync.Pool 25% of Put calls being dropped when race detector is enabled.
const decoderPoolReuseIterations = 30

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
	})

	t.Run("ResettableReader", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()
		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]

		var createCount, resetCount int
		creator.create = wrapCreate(creator.create, &createCount, &resetCount)

		for i := range decoderPoolReuseIterations {
			msg := fmt.Sprintf("test_data_%d", i)
			decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, compressGzip(msg))
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, msg, string(decoded))
		}

		require.Equal(t, decoderPoolReuseIterations, createCount+resetCount,
			"each decode must call exactly one of create or Reset")
		require.GreaterOrEqual(t, createCount, 1, "first decode must call create")
		require.Greater(t, resetCount, 0, "subsequent decodes must reuse pooled decoders via Reset")
	})

	t.Run("NotResettableReaderCustom", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		customCodec := rawtopiccommon.Codec(1001)

		var createCount, resetCount int
		testMultiDecoder.AddDecoder(customCodec, wrapCreate(
			func(r io.Reader) (io.Reader, error) {
				rd, err := gzip.NewReader(r)
				if err != nil {
					return nil, err
				}

				return &notResettableReader{Reader: rd}, nil
			},
			&createCount, &resetCount,
		))
		require.Len(t, testMultiDecoder.m, defaultDecodersCount+1)

		for i := range decoderPoolReuseIterations {
			msg := fmt.Sprintf("test_data_%d", i)
			decodedReader, err := testMultiDecoder.Decode(customCodec, compressGzip(msg))
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, msg, string(decoded))
		}

		require.Equal(t, decoderPoolReuseIterations, createCount,
			"non-resettable decoder must be created on every decode")
		require.Zero(t, resetCount, "non-resettable decoder must never be reused via Reset")
	})

	t.Run("ResettableReaderCustom", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

		customCodec := rawtopiccommon.Codec(1001)

		var createCount, resetCount int
		testMultiDecoder.AddDecoder(customCodec, wrapCreate(
			func(r io.Reader) (io.Reader, error) {
				return gzip.NewReader(r)
			},
			&createCount, &resetCount,
		))
		require.Len(t, testMultiDecoder.m, defaultDecodersCount+1)

		for i := range decoderPoolReuseIterations {
			msg := fmt.Sprintf("test_data_%d", i)
			decodedReader, err := testMultiDecoder.Decode(customCodec, compressGzip(msg))
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, msg, string(decoded))
		}

		require.Equal(t, decoderPoolReuseIterations, createCount+resetCount,
			"each decode must call exactly one of create or Reset")
		require.GreaterOrEqual(t, createCount, 1, "first decode must call create")
		require.Greater(t, resetCount, 0,
			"custom resettable decoder must be reused via Reset on subsequent decodes")
	})

	t.Run("ResettableReaderManyMessages", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()
		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]

		var createCount, resetCount int
		creator.create = wrapCreate(creator.create, &createCount, &resetCount)

		const iterations = 50
		for i := range iterations {
			testMsg := fmt.Sprintf("test_data_%d", i)
			encodedReader := compressGzip(testMsg)

			decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, encodedReader)
			require.NoError(t, err)

			decoded, err := io.ReadAll(decodedReader)
			require.NoError(t, err)
			require.Equal(t, testMsg, string(decoded))
		}

		require.Equal(t, iterations, createCount+resetCount,
			"each decode must call exactly one of create or Reset")
		require.Greater(t, resetCount, 0, "pooled decoders must be reused via Reset")
	})

	t.Run("CodecRawDoesNotPoolResettableInput", func(t *testing.T) {
		testMultiDecoder := NewMultiDecoder()

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
		creator := testMultiDecoder.m[rawtopiccommon.CodecGzip]

		var createCount, resetCount int
		creator.create = wrapCreate(creator.create, &createCount, &resetCount)

		decodedReader, err := testMultiDecoder.Decode(rawtopiccommon.CodecGzip, compressGzip("payload"))
		require.NoError(t, err)

		decoded, err := io.ReadAll(decodedReader)
		require.NoError(t, err)
		require.Equal(t, "payload", string(decoded))

		require.Equal(t, 1, createCount, "first decode must call create exactly once")
		require.Zero(t, resetCount, "first decode must not Reset anything")

		buf := make([]byte, 16)
		for range 3 {
			n, err := decodedReader.Read(buf)
			require.Zero(t, n)
			require.ErrorIs(t, err, io.EOF)
		}

		require.Equal(t, 1, createCount, "extra reads after EOF must not trigger create")
		require.Zero(t, resetCount, "extra reads after EOF must not trigger Reset")
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

	resetCalls *int
}

func (r *resettableReaderTracker) Reset(input io.Reader) error {
	*r.resetCalls++

	if rd, ok := r.Reader.(PublicResettableReader); ok {
		return rd.Reset(input)
	}

	return nil
}

func wrapCreate(
	create PublicCreateDecoderFunc,
	createCount, resetCount *int,
) PublicCreateDecoderFunc {
	return func(input io.Reader) (io.Reader, error) {
		*createCount++

		dec, err := create(input)
		if err != nil {
			return nil, err
		}

		if rd, ok := dec.(PublicResettableReader); ok {
			return &resettableReaderTracker{
				Reader:     rd,
				resetCalls: resetCount,
			}, nil
		}

		return dec, nil
	}
}
