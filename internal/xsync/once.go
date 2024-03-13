package xsync

import (
	"context"
	"sync"
)

func OnceFunc(f func(ctx context.Context) error) func(ctx context.Context) error {
	var once sync.Once

	return func(ctx context.Context) (err error) {
		once.Do(func() {
			err = f(ctx)
		})

		return err
	}
}

func OnceValue[T any](f func(ctx context.Context) (T, error)) func(ctx context.Context) (T, error) {
	var (
		once  sync.Once
		mutex sync.RWMutex
		t     T
		err   error
	)

	return func(ctx context.Context) (T, error) {
		once.Do(func() {
			mutex.Lock()
			defer mutex.Unlock()

			t, err = f(ctx)
		})

		mutex.RLock()
		defer mutex.RUnlock()

		return t, err
	}
}
