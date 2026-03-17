package xslices

func Subtract[T comparable](source []T, toSubtract []T) []T {
	if len(toSubtract) == 0 {
		return source
	}

	var (
		counts     = make(map[T]struct{}, len(source)+len(toSubtract))
		difference = make([]T, 0, len(source))
	)

	for _, v := range toSubtract {
		counts[v] = struct{}{}
	}

	for _, v := range source {
		if _, exists := counts[v]; exists {
			continue
		}
		difference = append(difference, v)
	}

	return difference
}
