package xsync

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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

func TestOnceValue(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("Race", func(t *testing.T) {
		counter := 0
		once := OnceValue(func() (*testCloser, error) {
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
		once := OnceValue(func() (*testCloser, error) {
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
		once := OnceValue(func() (*testCloser, error) {
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
