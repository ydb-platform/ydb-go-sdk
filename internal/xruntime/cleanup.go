//go:build !go1.24

package xruntime

import "runtime"

func AddCleanup[T, S any](ptr *T, cleanup func(S), arg S) {
	runtime.SetFinalizer(ptr, func(ptr *T) {
		cleanup(arg)
	})
}
