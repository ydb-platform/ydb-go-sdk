package xsync

import (
	"context"
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

func TestOnceValue(t *testing.T) {
	var (
		ctx = xtest.Context(t)
		cnt = 0
	)
	f := OnceValue(func(ctx context.Context) (int, error) {
		cnt++

		return cnt, nil
	})
	require.Equal(t, 0, cnt)
	var wg sync.WaitGroup
	wg.Add(1000)
	for range make([]struct{}, 1000) {
		go func() {
			defer wg.Done()
			v, err := f(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, v)
		}()
	}
	wg.Wait()
}
