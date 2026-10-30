package topicwriterinternal

import (
	"bytes"
	"compress/gzip"
	"io"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestEncoderOptionSharedAcrossWriters(t *testing.T) {
	option := WithAddEncoder(rawtopiccommon.CodecGzip, func(output io.Writer) (io.WriteCloser, error) {
		return gzip.NewWriter(output), nil
	})

	writers := []*WriterReconnector{
		newTestWriterStopped(option),
		newTestWriterStopped(option),
		newTestWriterStopped(option),
	}
	for _, writer := range writers {
		require.Same(t, writers[0].cfg.AdditionalEncoders[rawtopiccommon.CodecGzip],
			writer.cfg.AdditionalEncoders[rawtopiccommon.CodecGzip])
	}
	for round := range 3 {
		for _, writer := range writers {
			var output bytes.Buffer
			payload := bytes.Repeat([]byte{byte(round)}, 100)
			_, err := writer.encodersMap.EncodeBytes(rawtopiccommon.CodecGzip, &output, payload)
			require.NoError(t, err)
			reader, err := gzip.NewReader(&output)
			require.NoError(t, err)
			decoded, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, payload, decoded)
			runtime.GC()
			runtime.GC()
		}
	}
}

func TestSeparateEncoderOptionsStayIndependent(t *testing.T) {
	newOption := func(prefix string) PublicWriterOption {
		return WithAddEncoder(rawtopiccommon.CodecRaw, func(output io.Writer) (io.WriteCloser, error) {
			return &prefixedTestEncoder{output: output, prefix: prefix}, nil
		})
	}
	first := newTestWriterStopped(newOption("first:"))
	second := newTestWriterStopped(newOption("second:"))
	for range 3 {
		var firstOutput, secondOutput bytes.Buffer
		_, err := first.encodersMap.EncodeBytes(rawtopiccommon.CodecRaw, &firstOutput, []byte("message"))
		require.NoError(t, err)
		_, err = second.encodersMap.EncodeBytes(rawtopiccommon.CodecRaw, &secondOutput, []byte("message"))
		require.NoError(t, err)
		require.Equal(t, "first:message", firstOutput.String())
		require.Equal(t, "second:message", secondOutput.String())
	}
}

type prefixedTestEncoder struct {
	output io.Writer
	prefix string
}

func (w *prefixedTestEncoder) Write(data []byte) (int, error) {
	if _, err := io.WriteString(w.output, w.prefix); err != nil {
		return 0, err
	}

	return w.output.Write(data)
}

func (w *prefixedTestEncoder) Close() error { return nil }

func (w *prefixedTestEncoder) Reset(output io.Writer) { w.output = output }
