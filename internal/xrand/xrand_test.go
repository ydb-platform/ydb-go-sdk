package xrand

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCryptoSeeded(t *testing.T) {
	t.Run("DifferentEntropyProducesDifferentSequence", func(t *testing.T) {
		first, err := newCryptoSeeded(bytes.NewReader([]byte{1, 0, 0, 0, 0, 0, 0, 0}))
		require.NoError(t, err)
		second, err := newCryptoSeeded(bytes.NewReader([]byte{2, 0, 0, 0, 0, 0, 0, 0}))
		require.NoError(t, err)

		firstSequence := []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		secondSequence := append([]int(nil), firstSequence...)
		first.Shuffle(len(firstSequence), func(i, j int) {
			firstSequence[i], firstSequence[j] = firstSequence[j], firstSequence[i]
		})
		second.Shuffle(len(secondSequence), func(i, j int) {
			secondSequence[i], secondSequence[j] = secondSequence[j], secondSequence[i]
		})

		require.NotEqual(t, firstSequence, secondSequence)
	})

	t.Run("EntropyError", func(t *testing.T) {
		expectedErr := errors.New("entropy unavailable")

		rnd, err := newCryptoSeeded(errorReader{err: expectedErr})

		require.Nil(t, rnd)
		require.ErrorIs(t, err, expectedErr)
	})
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
