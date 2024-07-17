//go:build !go1.21
// +build !go1.21

package xslices

import (
	"sort"
)

func Sort[T any](in []T, cmp func(lhs, rhs T) int) (out []T) {
	out = Clone(in)

	sort.Slice(out, func(i, j int) bool {
		return cmp(out[i], out[j]) < 0
	})

	return out
}
