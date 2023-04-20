package log

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"

func joinScope(names []string, maxLen int) (join string) {
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
			b.WriteRune([]rune(s)[0])
			l += 1 - ll
		} else {
			b.WriteString(s)
		}
	}
	return b.String()
}
