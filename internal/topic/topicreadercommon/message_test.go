package topicreadercommon

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func TestPublicMessage(t *testing.T) {
	t.Run("DecoderClosesAfterFullRead", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		gzipReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		msg := &PublicMessage{data: newOneTimeReaderFromReader(gzipReader)}

		_, err = io.ReadAll(msg)
		require.NoError(t, err, "ReadAll() should complete without errors")

		_, err = gzipReader.Read([]byte{0})
		require.Error(t, err, "gzip.Reader should be closed after full read")
	})

	t.Run("DecoderNotClosedBeforeEOF", func(t *testing.T) {
		data := []byte("test")
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		gzipReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		msg := &PublicMessage{data: newOneTimeReaderFromReader(gzipReader)}

		readBuf := make([]byte, 10)
		n, err := msg.Read(readBuf)
		require.Equal(t, io.EOF, err, "gzip.Reader returns EOF immediately after last byte")
		require.Equal(t, len(data), n, "should read all data at once")

		require.NoError(t, msg.Close(), "explicit Close after EOF should succeed")
	})

	t.Run("ReadAfterCloseReturnsEOF", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		gzipReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		msg := &PublicMessage{data: newOneTimeReaderFromReader(gzipReader)}

		require.NoError(t, msg.Close(), "Close() should execute without errors")

		readBuf := make([]byte, 2)
		n, err := msg.Read(readBuf)
		require.Equal(t, 0, n, "After Close(), Read() should return 0 bytes")
		require.Equal(t, io.EOF, err, "After Close(), Read() should return EOF")
	})

	t.Run("DecoderClosesAfterReadToEOF", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, err := gw.Write([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, gw.Close())

		gzipReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		msg := &PublicMessage{data: newOneTimeReaderFromReader(gzipReader)}

		_, err = io.ReadAll(msg)
		require.NoError(t, err)

		_, err = gzipReader.Read([]byte{0})
		require.Error(t, err, "gzip.Reader should be closed after full read to EOF")
	})

	t.Run("DecoderReuseFromPool", func(t *testing.T) {
		dm := NewDecoderMap()
		customCodec := rawtopiccommon.Codec(1006)

		dm.AddDecoder(customCodec, func(input io.Reader) (ReadResetter, error) {
			return gzip.NewReader(input)
		})

		var buf1 bytes.Buffer
		gw1 := gzip.NewWriter(&buf1)
		_, err := gw1.Write([]byte("message"))
		require.NoError(t, err)
		require.NoError(t, gw1.Close())

		reader1, err := dm.Decode(customCodec, &buf1)
		require.NoError(t, err)

		data1, err := io.ReadAll(reader1)
		require.NoError(t, err)
		require.Equal(t, "message", string(data1))

		require.NoError(t, reader1.(io.Closer).Close())

		pool := dm.dp[customCodec]
		reusedDecoder := pool.Get()
		require.NotNil(t, reusedDecoder, "Decoder should be retrieved from pool after Close")

		pool.Put(reusedDecoder)

		var buf2 bytes.Buffer
		gw2 := gzip.NewWriter(&buf2)
		_, err = gw2.Write([]byte("message2"))
		require.NoError(t, err)
		require.NoError(t, gw2.Close())

		reader2, err := dm.Decode(customCodec, &buf2)
		require.NoError(t, err)

		data2, err := io.ReadAll(reader2)
		require.NoError(t, err)
		require.Equal(t, "message2", string(data2))

		require.NoError(t, reader2.(io.Closer).Close())
	})
}
