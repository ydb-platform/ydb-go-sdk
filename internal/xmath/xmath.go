package xmath

import "cmp"

func Min[T cmp.Ordered](v T, values ...T) T {
	for _, value := range values {
		if value < v {
			v = value
		}
	}
	return v
}

func Max[T cmp.Ordered](v T, values ...T) T {
	for _, value := range values {
		if value > v {
			v = value
		}
	}
	return v
}
