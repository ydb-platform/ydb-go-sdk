package xslices

func Subtract[T comparable](source []T, toSubtract []T) []T {
	if len(toSubtract) == 0 {
		return source
	}

	var (
		excludeSet = make(map[T]struct{}, len(source)+len(toSubtract))
		difference = make([]T, 0, len(source))
	)

	for _, v := range toSubtract {
		excludeSet[v] = struct{}{}
	}

	for _, v := range source {
		if _, exists := excludeSet[v]; exists {
			continue
		}
		difference = append(difference, v)
	}

	return difference
}
