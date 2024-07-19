//go:build go1.21
// +build go1.21

package xslices

import (
	"slices"
)

func Sort[T any](in []T, cmp func(lhs, rhs T) int) {
	slices.SortFunc(in, cmp)
}
