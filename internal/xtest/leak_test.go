package xtest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckGoroutinesLeak(t *testing.T) {
	t.Run("Leak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			ch := make(chan struct{})
			require.Panics(t, func() {
				defer checkGoroutinesLeak(func([]string) {
					panic("test")
				})
				go func() {
					<-ch
				}()
			})
			close(ch)
		})
	})
	t.Run("NoLeak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			require.NotPanics(t, func() {
				defer checkGoroutinesLeak(func([]string) {
					panic("test")
				})
				ch := make(chan struct{})
				defer func() {
					<-ch
				}()
				go func() {
					close(ch)
				}()
			})
		})
	})
}
