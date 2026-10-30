package topicwritercommon

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoderDiscardsFailures(t *testing.T) {
	failure := errors.New("encoder failure")
	for _, stage := range []string{"create", "write", "close"} {
		t.Run(stage, func(t *testing.T) {
			var created int
			encoder := NewEncoder(func(output io.Writer) (io.WriteCloser, error) {
				created++
				w := &poolTestWriter{output: output}
				if created == 1 {
					switch stage {
					case "create":
						return nil, failure
					case "write":
						w.writeErr = failure
					case "close":
						w.closeErr = failure
					}
				}

				return w, nil
			})
			_, err := encoder.EncodeBytes(io.Discard, []byte("payload"))
			require.ErrorIs(t, err, failure)
			_, err = encoder.EncodeBytes(io.Discard, []byte("retry"))
			require.NoError(t, err)
			require.Equal(t, 2, created)
		})
	}
}

func TestEncoderConcurrentReuse(t *testing.T) {
	t.Parallel()

	encoder := NewEncoder(func(output io.Writer) (io.WriteCloser, error) {
		return &poolTestWriter{output: output}, nil
	})
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			for j := range 10 {
				payload := fmt.Sprintf("%d:%d", i, j)
				var output bytes.Buffer
				_, err := encoder.EncodeBytes(&output, []byte(payload))
				require.NoError(t, err)
				require.Equal(t, payload, output.String())
			}
		})
	}
}

func TestEncoderWithoutReset(t *testing.T) {
	var created int
	encoder := NewEncoder(func(output io.Writer) (io.WriteCloser, error) {
		created++

		return nopWriteCloser{output}, nil
	})
	for range 2 {
		_, err := encoder.EncodeBytes(io.Discard, []byte("payload"))
		require.NoError(t, err)
	}
	require.Equal(t, 2, created)
}

type poolTestWriter struct {
	output   io.Writer
	writeErr error
	closeErr error
}

func (w *poolTestWriter) Write(data []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}

	return w.output.Write(data)
}

func (w *poolTestWriter) Close() error { return w.closeErr }

func (w *poolTestWriter) Reset(output io.Writer) { w.output = output }
