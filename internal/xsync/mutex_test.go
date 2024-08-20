package xsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestMutex(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
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
}

func TestRWMutex(t *testing.T) {
	xtest.TestManyTimesWithName(t, "WithLock", func(t testing.TB) {
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
	xtest.TestManyTimesWithName(t, "WithRLock", func(t testing.TB) {
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

func TestWithLock(t *testing.T) {
	t.Run("sync.Mutex", func(t *testing.T) {
		mtx := sync.Mutex{}
		v := WithLock(&mtx, func() int {
			return 123
		})
		require.Equal(t, 123, v)
	})
	t.Run("xsync.Mutex", func(t *testing.T) {
		mtx := Mutex{}
		v := WithLock(&mtx, func() int {
			return 123
		})
		require.Equal(t, 123, v)
	})
}

func TestWithRLock(t *testing.T) {
	t.Run("sync.RWMutex", func(t *testing.T) {
		mtx := sync.RWMutex{}
		v := WithRLock(&mtx, func() int {
			return 123
		})
		require.Equal(t, 123, v)
	})
	t.Run("xsync.RWMutex", func(t *testing.T) {
		mtx := RWMutex{}
		v := WithRLock(&mtx, func() int {
			return 123
		})
		require.Equal(t, 123, v)
	})
}
