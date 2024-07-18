package xslices

func Filter[T any](x []T, filter func(t T) bool) (good, bad []T) {
	good = make([]T, 0, len(x))
	bad = make([]T, 0, len(x))

	for i := 0; i < len(x); i++ {
		if filter(x[i]) {
			good = append(good, x[i])
		} else {
			bad = append(bad, x[i])
		}
	}

	return good, bad
}
