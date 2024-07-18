package xslices

func SortCopy[T any](in []T, cmp func(lhs, rhs T) int) (out []T) {
	out = Clone(in)

	Sort(out, cmp)

	return out
}
