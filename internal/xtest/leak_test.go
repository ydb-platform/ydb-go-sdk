package xtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckGoroutinesLeak(t *testing.T) {
	t.Run("Leak", func(t *testing.T) {
		require.Panics(t, func() {
			defer checkGoroutinesLeak(func(stacks []string) {
				panic("panic")
			})
			go func() {
				time.Sleep(time.Second)
			}()
		})
	})
	t.Run("NoLeak", func(t *testing.T) {
		require.NotPanics(t, func() {
			defer checkGoroutinesLeak(func(stacks []string) {
				panic("panic")
			})
			time.Sleep(time.Second)
		})
	})
}
