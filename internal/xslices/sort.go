//go:build go1.21
// +build go1.21

package xslices

import (
	"slices"
)

func SortCopy[T any](in []T, cmp func(lhs, rhs T) int) (out []T) {
	out = Clone(in)

	Sort(out, cmp)

	return out
}

func Sort[T any](in []T, cmp func(lhs, rhs T) int) {
	slices.SortFunc(in, cmp)
}
