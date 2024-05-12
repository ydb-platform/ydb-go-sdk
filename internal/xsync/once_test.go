package xsync

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

var errEmpty = errors.New("")

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

func TestOnceValue(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("Race", func(t *testing.T) {
		counter := 0
		once := OnceValue(func() *testCloser {
			counter++

			return &testCloser{value: counter}
		})
		var wg sync.WaitGroup
		wg.Add(1000)
		for range make([]struct{}, 1000) {
			go func() {
				defer wg.Done()
				v := once.Get()
				require.Equal(t, 1, v.value)
			}()
		}
		wg.Wait()
	})
	t.Run("GetBeforeClose", func(t *testing.T) {
		once := OnceValue(func() *testCloser {
			return &testCloser{
				inited:   true,
				closeErr: errEmpty,
			}
		})
		v := once.Get()
		require.True(t, v.inited)
		require.False(t, v.closed)
		err := once.Close(ctx)
		require.ErrorIs(t, err, errEmpty)
		require.True(t, v.inited)
		require.True(t, v.closed)
	})
	t.Run("CloseBeforeGet", func(t *testing.T) {
		once := OnceValue(func() *testCloser {
			return &testCloser{
				inited:   true,
				closeErr: errEmpty,
			}
		})
		err := once.Close(ctx)
		require.NoError(t, err)
		v := once.Get()
		require.Nil(t, v)
	})
}
