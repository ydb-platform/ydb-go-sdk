package topicreadercommon

import (
	"bytes"
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

		// first read is ok
		firstBuf := make([]byte, bufSize)
		n, err := r.Read(firstBuf)
		require.NoError(t, err)
		require.Equal(t, bufSize, n)
		require.Equal(t, preparedData[:bufSize], firstBuf)
		require.NoError(t, err)

		// iotest.TimeoutReader return timeout for second read
		secondBuf := make([]byte, bufSize)
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
		require.Equal(t, make([]byte, bufSize), secondBuf)

		// Next read again
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
	})
}
