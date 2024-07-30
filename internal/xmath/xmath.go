package xmath

func Min[T ordered](v T, values ...T) T {
	for _, value := range values {
		if value < v {
			v = value
		}
	}

	return v
}

func Max[T ordered](v T, values ...T) T {
	for _, value := range values {
		if value > v {
			v = value
		}
	}

	return v
}
