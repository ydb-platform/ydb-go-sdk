package xsync

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestOnceFunc(t *testing.T) {
	var (
		ctx      = xtest.Context(t)
		cnt      = 0
		constErr = errors.New("")
	)
	f := OnceFunc(func(ctx context.Context) error {
		cnt++

		return constErr
	})
	require.Equal(t, 0, cnt)
	require.ErrorIs(t, f(ctx), constErr)
	require.Equal(t, 1, cnt)
	require.ErrorIs(t, f(ctx), constErr)
	require.Equal(t, 1, cnt)
}

type testCloser struct {
	value      int
	inited     bool
	closed     bool
	closeCalls int
	closeErr   error
}

func (c *testCloser) Close(ctx context.Context) error {
	c.closed = true
	c.closeCalls++

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
	t.Run("GetErrorBeforeClose", func(t *testing.T) {
		constInitErr := errors.New("")
		once := OnceValue(func() (*testCloser, error) {
			return nil, constInitErr
		})
		v, err := once.Get()
		require.Nil(t, v)
		require.ErrorIs(t, err, constInitErr)
		require.NotPanics(t, func() {
			require.NoError(t, once.Close(ctx))
		})
	})
	t.Run("GetValueAndErrorBeforeClose", func(t *testing.T) {
		constInitErr := errors.New("")
		value := &testCloser{inited: true}
		once := OnceValue(func() (*testCloser, error) {
			return value, constInitErr
		})
		v, err := once.Get()
		require.Same(t, value, v)
		require.ErrorIs(t, err, constInitErr)
		require.NoError(t, once.Close(ctx))
		require.True(t, value.closed)
	})
	t.Run("CloseIsIdempotent", func(t *testing.T) {
		constCloseErr := errors.New("")
		value := &testCloser{
			inited:   true,
			closeErr: constCloseErr,
		}
		once := OnceValue(func() (*testCloser, error) {
			return value, nil
		})
		_, err := once.Get()
		require.NoError(t, err)
		require.ErrorIs(t, once.Close(ctx), constCloseErr)
		require.ErrorIs(t, once.Close(ctx), constCloseErr)
		require.Equal(t, 1, value.closeCalls)
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
