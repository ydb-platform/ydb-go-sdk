package xsync

import (
	"context"
	"reflect"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
)

func OnceFunc(f func(ctx context.Context) error) func(ctx context.Context) error {
	var (
		once sync.Once
		err  error
	)

	return func(ctx context.Context) error {
		once.Do(func() {
			err = f(ctx)
		})

		return err
	}
}

type onceResult[T closer.Closer] struct {
	value   T
	initErr error
	close   func(context.Context) error
}

type Once[T closer.Closer] struct {
	f      func() (T, error)
	once   sync.Once
	result *onceResult[T]
}

func OnceValue[T closer.Closer](f func() (T, error)) *Once[T] {
	return &Once[T]{f: f}
}

func (v *Once[T]) Close(ctx context.Context) error {
	v.once.Do(func() {})

	if v.result == nil || v.result.close == nil {
		return nil
	}

	return v.result.close(ctx)
}

func (v *Once[T]) Get() (T, error) {
	v.once.Do(func() {
		value, err := v.f()
		v.result = &onceResult[T]{
			value:   value,
			initErr: err,
		}
		if !isNil(value) {
			v.result.close = OnceFunc(value.Close)
		}
	})

	if v.result == nil {
		var zero T

		return zero, nil
	}

	return v.result.value, v.result.initErr
}

func (v *Once[T]) Must() T {
	t, err := v.Get()
	if err != nil {
		panic(err)
	}

	return t
}

func isNil[T any](v T) bool {
	value := reflect.ValueOf(v)
	if !value.IsValid() {
		return true
	}

	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
