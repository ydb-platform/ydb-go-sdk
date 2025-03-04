package xtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckGoroutinesLeak(t *testing.T) {
	t.Run("Leak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			ch := make(chan struct{})
			require.Panics(t, func() {
				defer checkGoroutinesLeak(func(goroutines []string) {
					panic("panic")
				})
				go func() {
					<-ch
				}()
			})
			close(ch)
		}, StopAfter(13*time.Second))
	})
	t.Run("NoLeak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			ch := make(chan struct{})
			require.NotPanics(t, func() {
				defer checkGoroutinesLeak(func(goroutines []string) {
					panic("panic")
				})
				defer func() {
					<-ch
				}()
				go func() {
					close(ch)
				}()
			})
		}, StopAfter(13*time.Second))
	})
}
