package xtest

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckGoroutinesLeak(t *testing.T) {
	t.Run("Leak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			var (
				leakDetected atomic.Bool
				ch           = make(chan struct{})
			)
			func() {
				defer func() {
					if err := findGoroutinesLeak(); err != nil {
						leakDetected.Store(true)
					}
				}()
				go func() {
					<-ch
				}()
			}()
			close(ch)
			require.True(t, leakDetected.Load())
		})
	})
	t.Run("NoLeak", func(t *testing.T) {
		TestManyTimes(t, func(t testing.TB) {
			var (
				leakDetected atomic.Bool
				ch           = make(chan struct{})
			)
			func() {
				defer func() {
					if err := findGoroutinesLeak(); err != nil {
						leakDetected.Store(true)
					}
				}()
				defer func() {
					<-ch
				}()
				go func() {
					close(ch)
				}()
			}()
			require.False(t, leakDetected.Load())
		})
	})
}
