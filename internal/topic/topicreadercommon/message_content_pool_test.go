package topicreadercommon

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func BenchmarkConsumeContent(b *testing.B) {
	b.ReportAllocs()
	content := []byte("asd")
	reader := bytes.NewReader(content)
	msg := PublicMessage{data: newOneTimeReaderFromReader(reader)}
	for i := 0; i < b.N; i++ {
		reader.Reset(content)
		msg.dataConsumed = false
		msg.data = newOneTimeReaderFromReader(reader)
		err := msg.UnmarshalTo(emptyConsumer{})
		if err != nil {
			b.Fatal()
		}
	}
}

func TestCallbackOnReaderContent(t *testing.T) {
	expectedData := []byte(`asdf`)
	newReader := func() io.Reader {
		return bytes.NewReader(expectedData)
	}

	t.Run("ZeroEstimatedSize", func(t *testing.T) {
		mc := gomock.NewController(t)
		p := NewMockPool(mc)
		p.EXPECT().Get().Return(nil)
		p.EXPECT().Put(gomock.Any()).Do(func(val any) {
			buf := val.(*bytes.Buffer)
			require.NotNil(t, buf)
			require.Equal(t, 0, buf.Len())
			require.Equal(t, minInitializeBufferSize, buf.Cap())
		})

		called := false
		err := callbackOnReaderContent(p, newReader(), 0, testFuncConsumer(func(data []byte) error {
			called = true
			require.Equal(t, expectedData, data)

			return nil
		}))
		require.NoError(t, err)
		require.True(t, called)
	})
	t.Run("MiddleEstimatedSize", func(t *testing.T) {
		estimatedSize := minInitializeBufferSize + 10

		mc := gomock.NewController(t)
		p := NewMockPool(mc)
		p.EXPECT().Get().Return(nil).Times(2)

		var targetCapacity int
		p.EXPECT().Put(gomock.Any()).DoAndReturn(func(val any) {
			buf := val.(*bytes.Buffer)
			targetCapacity = buf.Cap()
		})

		p.EXPECT().Put(gomock.Any()).DoAndReturn(func(val any) {
			buf := val.(*bytes.Buffer)
			require.NotNil(t, buf)
			require.Equal(t, 0, buf.Len())

			// check about target capacity same as without read - that mean no reallocations while read
			require.Equal(t, targetCapacity, buf.Cap())
		})

		// first call with empty reader - for check internal capacity without reallocation
		_ = callbackOnReaderContent(p, ErrReader(io.EOF), estimatedSize, testFuncConsumer(func(data []byte) error {
			require.Empty(t, data)

			return nil
		}))

		// real call
		called := false
		err := callbackOnReaderContent(p, newReader(), estimatedSize, testFuncConsumer(func(data []byte) error {
			require.Equal(t, expectedData, data)
			called = true

			return nil
		}))
		require.NoError(t, err)
		require.True(t, called)
	})
	t.Run("LargeEstimatedSize", func(t *testing.T) {
		mc := gomock.NewController(t)
		p := NewMockPool(mc)
		p.EXPECT().Get().Return(nil)
		p.EXPECT().Put(gomock.Any()).DoAndReturn(func(val any) {
			buf := val.(*bytes.Buffer)
			require.NotNil(t, buf)
			require.Equal(t, 0, buf.Len())
			require.Equal(t, maxInitialBufferSize, buf.Cap())
		})

		called := false
		err := callbackOnReaderContent(p, newReader(), maxInitialBufferSize+10, testFuncConsumer(func(data []byte) error {
			require.Equal(t, expectedData, data)
			called = true

			return nil
		}))
		require.NoError(t, err)
		require.True(t, called)
	})
	t.Run("UseBufferFromPool", func(t *testing.T) {
		poolBuf := &bytes.Buffer{}
		mc := gomock.NewController(t)
		p := NewMockPool(mc)
		p.EXPECT().Get().Return(poolBuf)
		p.EXPECT().Put(gomock.Any()).DoAndReturn(func(buf any) {
			require.Same(t, poolBuf, buf)
		})

		called := false
		err := callbackOnReaderContent(p, newReader(), 0, testFuncConsumer(func(data []byte) error {
			require.Equal(t, expectedData, data)
			called = true

			return nil
		}))
		require.NoError(t, err)
		require.True(t, called)
	})
	t.Run("ReturnErrorFromReader", func(t *testing.T) {
		testErr := errors.New("test")
		err := callbackOnReaderContent(globalReadMessagePool, ErrReader(testErr), 0, nil)
		require.ErrorIs(t, err, testErr)
		require.False(t, xerrors.IsYdb(err))
	})
	t.Run("ReturnErrorFromUnmarshal", func(t *testing.T) {
		testErr := errors.New("test")
		err := callbackOnReaderContent(
			globalReadMessagePool,
			ErrReader(io.EOF),
			0,
			testFuncConsumer(func(data []byte) error {
				return testErr
			}),
		)
		require.ErrorIs(t, err, testErr)
		require.False(t, xerrors.IsYdb(err))
	})
}

type emptyConsumer struct{}

func (emptyConsumer) UnmarshalYDBTopicMessage([]byte) error {
	return nil
}

type testFuncConsumer func([]byte) error

func (c testFuncConsumer) UnmarshalYDBTopicMessage(data []byte) error {
	return c(data)
}

// ErrReader returns an io.Reader that returns 0, err from all Read calls.
// copy for use with go pre 1.16
func ErrReader(err error) io.Reader {
	return &errReader{err: err}
}

type errReader struct {
	err error
}

func (r *errReader) Read(p []byte) (int, error) {
	return 0, r.err
}
