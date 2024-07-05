//go:build !go1.22 || !goexperiment.rangefunc

package xiter

type Seq2[K, V any] func(yield func(K, V) bool)
