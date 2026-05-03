package xslices

import (
	"cmp"
	"slices"
)

func Uniq[T cmp.Ordered](in []T) (out []T) {
	slices.Sort(in)

	out = make([]T, 0, len(in))
	out = append(out, in[0])

	for i := 1; i < len(in); i++ {
		if in[i] != out[len(out)-1] {
			out = append(out, in[i])
		}
	}

	return out
}
