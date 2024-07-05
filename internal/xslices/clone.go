//go:build go1.21
// +build go1.21

package xslices

import "slices"

// Clone returns a copy of the slice.
// The elements are copied using assignment, so this is a shallow clone.
func Clone[S ~[]E, E any](s S) S {
	return slices.Clone(s)
}
