//go:build !go1.21
// +build !go1.21

package xslices

import (
	"sort"
)

func SortCopy[T any](in []T, cmp func(lhs, rhs T) int) (out []T) {
	out = Clone(in)

	Sort(out, func(i, j int) bool {
		return cmp(out[i], out[j]) < 0
	})

	return out
}

func Sort[T any](in []T, cmp func(lhs, rhs T) int) {
	sort.Slice(in, func(i, j int) bool {
		return cmp(in[i], in[j]) < 0
	})
}
