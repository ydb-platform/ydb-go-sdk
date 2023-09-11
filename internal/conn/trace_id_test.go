package conn

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func Test_newTraceID(t *testing.T) {
	t.Run("good", func(t *testing.T) {
		traceID, err := newTraceID(
			func(opts *newTraceIDOpts) {
				opts.newRandom = func() (uuid.UUID, error) {
					return uuid.UUID{}, nil
				}
			},
		)
		require.NoError(t, err)
		require.Equal(t, "00000000-0000-0000-0000-000000000000", traceID)
	})
	t.Run("bad", func(t *testing.T) {
		_, err := newTraceID(
			func(opts *newTraceIDOpts) {
				opts.newRandom = func() (uuid.UUID, error) {
					return uuid.UUID{}, errors.New("")
				}
			},
		)
		require.Error(t, err)
	})
}
