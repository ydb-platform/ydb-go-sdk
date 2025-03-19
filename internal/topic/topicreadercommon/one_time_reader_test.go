package topicreadercommon

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestOneTimeReader(t *testing.T) {
	t.Run("FullRead", func(t *testing.T) {
		r := newOneTimeReaderFromReader(bytes.NewReader([]byte{1, 2, 3}))
		dstBuf := make([]byte, 3)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3}, dstBuf)
		_, err = r.Read(dstBuf)
		require.ErrorIs(t, err, io.EOF)
		require.Empty(t, r.reader)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("DstMoreThenContent", func(t *testing.T) {
		r := newOneTimeReaderFromReader(bytes.NewReader([]byte{1, 2, 3}))
		dstBuf := make([]byte, 4)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3, 0}, dstBuf)

		_, err = r.Read(dstBuf)
		require.ErrorIs(t, err, io.EOF)
		require.Empty(t, r.reader)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("ReadLess", func(t *testing.T) {
		r := newOneTimeReaderFromReader(bytes.NewReader([]byte{1, 2, 3}))
		dstBuf := make([]byte, 2)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{1, 2}, dstBuf)
		require.NotEmpty(t, r.reader)
		require.NoError(t, r.err)
	})
	t.Run("ReadAfterError", func(t *testing.T) {
		testErr := errors.New("err")
		r := &oneTimeReader{err: testErr}
		dstBuf := make([]byte, 2)
		n, err := r.Read(dstBuf)
		require.Equal(t, testErr, err)
		require.Equal(t, 0, n)
	})
	t.Run("InnerErr", func(t *testing.T) {
		r := newOneTimeReaderFromReader(nil)

		bufSize := 2
		preparedData := make([]byte, 2*bufSize)
		for i := 0; i < 2*bufSize; i++ {
			if i < bufSize {
				preparedData[i] = 1
			} else {
				preparedData[i] = 2
			}
		}
		r.reader = iotest.TimeoutReader(bytes.NewReader(preparedData))

		firstBuf := make([]byte, bufSize)
		n, err := r.Read(firstBuf)
		require.NoError(t, err)
		require.Equal(t, bufSize, n)
		require.Equal(t, preparedData[:bufSize], firstBuf)
		require.NoError(t, err)

		secondBuf := make([]byte, bufSize)
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
		require.Equal(t, make([]byte, bufSize), secondBuf)

		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
	})

	t.Run("CloseWithoutRead", func(t *testing.T) {
		reader := newOneTimeReaderFromReader(bytes.NewReader([]byte("test")))
		err := reader.Close()
		require.NoError(t, err)
	})

	t.Run("CloseTwice", func(t *testing.T) {
		reader := newOneTimeReaderFromReader(bytes.NewReader([]byte("test")))
		require.NoError(t, reader.Close())
		require.NoError(t, reader.Close())
	})

	t.Run("CloseReleasesResourcesWithGzipDecoder", func(t *testing.T) {
		data := []byte("test data for gzip")
		var buf bytes.Buffer
		gzWriter := gzip.NewWriter(&buf)
		_, err := gzWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzWriter.Close())

		gzReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		reader := newOneTimeReaderFromReader(gzReader)

		tmpBuf := make([]byte, 4)
		_, err = reader.Read(tmpBuf)
		require.NoError(t, err)

		err = reader.Close()
		require.NoError(t, err, "Close() should not return error for gzip.Reader")

		n, err := reader.Read(tmpBuf)
		require.Equal(t, 0, n, "After Close(), read should return 0 bytes")
		require.ErrorIs(t, err, io.EOF, "After Close(), read should return EOF")
	})

	t.Run("ReadAfterCloseReturnsEOFWithGzip", func(t *testing.T) {
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write([]byte("gzip data"))
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		gzipReader, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		r := newOneTimeReaderFromReader(gzipReader)
		require.NoError(t, r.Close(), "Close() should succeed")

		readBuf := make([]byte, 2)
		n, err := r.Read(readBuf)
		require.Equal(t, 0, n, "Read() after Close() should return 0 bytes")
		require.Equal(t, io.EOF, err, "Read() after Close() should return EOF")
	})

	t.Run("GzipDecoderReturnedToPoolAfterClose", func(t *testing.T) {
		pool := newDecoderPool()

		data := []byte("pool reuse test")
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		_, err := gzipWriter.Write(data)
		require.NoError(t, err)
		require.NoError(t, gzipWriter.Close())

		decoder, err := gzip.NewReader(&buf)
		require.NoError(t, err)

		reader := newOneTimeReaderFromReader(decoder)
		require.NoError(t, reader.Close(), "Close() should not return error")

		pool.Put(decoder)

		reusedDecoder := pool.Get()
		require.NotNil(t, reusedDecoder, "Decoder should be retrieved from pool after Close")
		require.Equal(t, decoder, reusedDecoder, "Same gzip.Reader instance should be reused from pool")
	})

}
