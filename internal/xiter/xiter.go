//go:build !go1.23

package xiter

type Seq2[K, V any] func(yield func(K, V) bool)
