//go:build go1.22 && goexperiment.rangefunc

package xiter

import (
	"iter"
)

type Seq2[K, V any] iter.Seq2[K, V]
