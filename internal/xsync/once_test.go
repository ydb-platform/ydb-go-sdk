package xsync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestOnceFunc(t *testing.T) {
	var (
		ctx = xtest.Context(t)
		cnt = 0
	)
	f := OnceFunc(func(ctx context.Context) error {
		cnt++

		return nil
	})
	require.Equal(t, 0, cnt)
	require.NoError(t, f(ctx))
	require.Equal(t, 1, cnt)
	require.NoError(t, f(ctx))
	require.Equal(t, 1, cnt)
}

type testCloser struct {
	value    int
	inited   bool
	closed   bool
	closeErr error
}

func (c *testCloser) Close(ctx context.Context) error {
	c.closed = true

	return c.closeErr
}

func TestOnceCloserValue(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("Race", func(t *testing.T) {
		counter := 0
		once := OnceCloserValue(func() (*testCloser, error) {
			counter++

			return &testCloser{value: counter}, nil
		})
		var wg sync.WaitGroup
		wg.Add(1000)
		for range make([]struct{}, 1000) {
			go func() {
				defer wg.Done()
				v, err := once.Get()
				require.NoError(t, err)
				require.Equal(t, 1, v.value)
			}()
		}
		wg.Wait()
	})
	t.Run("GetBeforeClose", func(t *testing.T) {
		constCloseErr := errors.New("")
		once := OnceCloserValue(func() (*testCloser, error) {
			return &testCloser{
				inited:   true,
				closeErr: constCloseErr,
			}, nil
		})
		require.NotPanics(t, func() {
			v := once.Must()
			require.True(t, v.inited)
			require.False(t, v.closed)
			err := once.Close(ctx)
			require.ErrorIs(t, err, constCloseErr)
			require.True(t, v.inited)
			require.True(t, v.closed)
		})
	})
	t.Run("CloseBeforeGet", func(t *testing.T) {
		constCloseErr := errors.New("")
		once := OnceCloserValue(func() (*testCloser, error) {
			return &testCloser{
				inited:   true,
				closeErr: constCloseErr,
			}, nil
		})
		err := once.Close(ctx)
		require.NoError(t, err)
		v, err := once.Get()
		require.NoError(t, err)
		require.Nil(t, v)
	})
}

func TestOnce(t *testing.T) {
	t.Run("FirstCallReturnsValue", func(t *testing.T) {
		var once Once[int]
		v, err := once.Do(func() (int, error) {
			return 42, nil
		})
		require.NoError(t, err)
		require.Equal(t, 42, v)
	})

	t.Run("SubsequentCallsReturnCached", func(t *testing.T) {
		var (
			once  Once[int]
			calls int
		)
		v1, err := once.Do(func() (int, error) {
			calls++

			return 7, nil
		})
		require.NoError(t, err)
		require.Equal(t, 7, v1)

		v2, err := once.Do(func() (int, error) {
			calls++

			return 99, nil
		})
		require.NoError(t, err)
		require.Equal(t, 7, v2)
		require.Equal(t, 1, calls)
	})

	t.Run("ErrorIsCached", func(t *testing.T) {
		var (
			once    Once[int]
			calls   int
			testErr = errors.New("boom")
		)
		v1, err := once.Do(func() (int, error) {
			calls++

			return 0, testErr
		})
		require.ErrorIs(t, err, testErr)
		require.Equal(t, 0, v1)

		v2, err := once.Do(func() (int, error) {
			calls++

			return 123, nil
		})
		require.ErrorIs(t, err, testErr)
		require.Equal(t, 0, v2)
		require.Equal(t, 1, calls)
	})

	t.Run("ValueAndErrorTogether", func(t *testing.T) {
		var (
			once    Once[int]
			testErr = errors.New("partial")
		)
		v1, err := once.Do(func() (int, error) {
			return 5, testErr
		})
		require.ErrorIs(t, err, testErr)
		require.Equal(t, 5, v1)

		v2, err := once.Do(func() (int, error) {
			return 0, nil
		})
		require.ErrorIs(t, err, testErr)
		require.Equal(t, 5, v2)
	})

	t.Run("Race", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			var (
				once  Once[int]
				calls atomic.Int64
			)
			const goroutines = 1000

			var (
				wg    sync.WaitGroup
				start = make(chan struct{})
			)
			wg.Add(goroutines)
			for range make([]struct{}, goroutines) {
				go func() {
					defer wg.Done()
					<-start
					v, err := once.Do(func() (int, error) {
						calls.Add(1)

						return 17, nil
					})
					require.NoError(t, err)
					require.Equal(t, 17, v)
				}()
			}
			close(start)
			wg.Wait()
			require.Equal(t, int64(1), calls.Load())
		})
	})
}
