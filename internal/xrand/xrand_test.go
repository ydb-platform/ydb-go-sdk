package xrand

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithCryptoSeed(t *testing.T) {
	t.Run("DifferentEntropyProducesDifferentSequence", func(t *testing.T) {
		first := New(withCryptoSeed(bytes.NewReader([]byte{1, 0, 0, 0, 0, 0, 0, 0})))
		second := New(withCryptoSeed(bytes.NewReader([]byte{2, 0, 0, 0, 0, 0, 0, 0})))

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

	t.Run("EntropyErrorFallsBack", func(t *testing.T) {
		rnd := New(withCryptoSeed(errorReader{err: errors.New("entropy unavailable")}))

		require.NotNil(t, rnd)
		require.GreaterOrEqual(t, rnd.Int(10), 0)
	})

	t.Run("PublicOption", func(t *testing.T) {
		rnd := New(WithLock(), WithCryptoSeed())

		require.NotNil(t, rnd)
		require.GreaterOrEqual(t, rnd.Int(10), 0)
	})
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

var _ io.Reader = errorReader{}
