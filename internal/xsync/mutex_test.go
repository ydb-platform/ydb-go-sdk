package xsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMutex(t *testing.T) {
	for i := 0; i < 100; i++ {
		var m Mutex
		a, b := 1, 1

		var wg sync.WaitGroup
		f := func() {
			defer wg.Done()

			if a+b == 2 {
				a = 2
			} else {
				b = 2
			}
		}

		wg.Add(2)
		go m.WithLock(f)
		go m.WithLock(f)

		wg.Wait()
		require.Equal(t, 2, a)
		require.Equal(t, 2, b)
	}
}

func TestRWMutex(t *testing.T) {
	for testIteration := 0; testIteration < 100; testIteration++ {
		t.Run("WithLock", func(t *testing.T) {
			var m Mutex
			a, b := 1, 1

			var wg sync.WaitGroup
			f := func() {
				defer wg.Done()

				if a+b == 2 {
					a = 2
				} else {
					b = 2
				}
			}

			wg.Add(2)
			go m.WithLock(f)
			go m.WithLock(f)

			wg.Wait()
			require.Equal(t, 2, a)
			require.Equal(t, 2, b)
		})
		t.Run("WithRLock", func(t *testing.T) {
			var m RWMutex
			a, b := 1, 1

			var badSummCount int64
			var wg sync.WaitGroup

			for reader := 0; reader < 100; reader++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					for i := 0; i < 1000; i++ {
						m.WithRLock(func() {
							if a+b != 2 {
								atomic.AddInt64(&badSummCount, 1)
							}
						})
						runtime.Gosched()
					}
				}()
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				for i := 0; i < 100; i++ {
					m.WithLock(func() {
						a++
						b--
					})
					runtime.Gosched()
				}
			}()

			wg.Wait()
			require.Equal(t, 2, a+b)
			require.Equal(t, int64(0), badSummCount)
		})
	}
}
