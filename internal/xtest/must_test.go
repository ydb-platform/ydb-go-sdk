package xtest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMust(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		require.NotPanics(t, func() {
			v := Must(func() (int, error) {
				return 1, nil
			}())
			require.Equal(t, 1, v)
		})
	})
	t.Run("Panic", func(t *testing.T) {
		require.Panics(t, func() {
			_ = Must(func() (int, error) {
				return 0, errors.New("test")
			}())
		})
	})
}
