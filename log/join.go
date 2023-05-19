package log

import (
	"unicode/utf8"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
)

func joinNamespace(names []string, maxLen int) (join string) {
	l := 0
	for i, s := range names {
		l += len([]rune(s))
		if i != 0 {
			l++
		}
	}
	b := allocator.Buffers.Get()
	defer allocator.Buffers.Put(b)
	for i, s := range names {
		if i != 0 {
			b.WriteByte('.')
		}
		ll := len([]rune(s))
		if maxLen != 0 && l > maxLen {
			r, _ := utf8.DecodeRuneInString(s)
			b.WriteRune(r)
			l += 1 - ll
		} else {
			b.WriteString(s)
		}
	}
	return b.String()
}
