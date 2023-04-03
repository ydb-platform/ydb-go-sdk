package query

import "strings"

func indexAny(s string, ss ...string) (index int, substring string) {
	index = len(s) + 1
	for i := range ss {
		idx := strings.Index(s, ss[i])
		if idx >= 0 && idx < index {
			index = idx
			substring = ss[i]
		}
	}
	if index > len(s) {
		return -1, ""
	}
	return index, substring
}

func indexNotEscaped(s string, ss string) (index int) {
	for {
		idx := strings.Index(s, ss)
		switch {
		case idx < 0:
			return -1
		case idx <= 0:
			return index + idx
		default:
			if s[idx-1] == '\\' {
				s = s[idx+1:]
				index += idx + 1
			} else {
				return index + idx
			}
		}
	}
}
