//go:build !go1.21
// +build !go1.21

package xslices

import (
	"sort"
)

func Sort[T any](in []T, cmp func(lhs, rhs T) int) {
	sort.Slice(in, func(i, j int) bool {
		return cmp(in[i], in[j]) < 0
	})
}
