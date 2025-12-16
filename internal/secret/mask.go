package secret

func Mask(s string) string {
	var (
		runes         = []rune(s)
		startPosition = 1
		endPosition   = len(runes) - 1
	)

	if len(runes) < 5 {
		startPosition = 0
		endPosition = len(runes)
	}

	for i := startPosition; i < endPosition; i++ {
		runes[i] = '*'
	}

	return string(runes)
}
