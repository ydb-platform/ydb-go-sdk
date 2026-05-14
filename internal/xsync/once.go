package xsync

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

type Once[T any] struct {
	once sync.Once

	value T
	err   error
}

func (c *Once[T]) Do(f func() (res T, err error)) (T, error) {
	c.once.Do(func() {
		c.value, c.err = f()
	})

	return c.value, c.err
}

func OnceFunc(f func(ctx context.Context) error) func(ctx context.Context) error {
	var once sync.Once

	return func(ctx context.Context) (err error) {
		once.Do(func() {
			err = f(ctx)
		})

		return err
	}
}

type OnceCloser[T closer.Closer] struct {
	f     func() (T, error)
	once  sync.Once
	mutex sync.RWMutex
	t     T
	err   error
}

func OnceCloserValue[T closer.Closer](f func() (T, error)) *OnceCloser[T] {
	return &OnceCloser[T]{f: f}
}

func (v *OnceCloser[T]) Close(ctx context.Context) (err error) {
	has := true
	v.once.Do(func() {
		has = false
	})

	if has {
		v.mutex.RLock()
		defer v.mutex.RUnlock()

		return v.t.Close(ctx)
	}

	return nil
}

func (v *OnceCloser[T]) Get() (T, error) {
	v.once.Do(func() {
		v.mutex.Lock()
		defer v.mutex.Unlock()

		v.t, v.err = v.f()
	})

	v.mutex.RLock()
	defer v.mutex.RUnlock()

	return v.t, v.err
}

func (v *OnceCloser[T]) Must() T {
	t, err := v.Get()
	if err != nil {
		panic(err)
	}

	return t
}
