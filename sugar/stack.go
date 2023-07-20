package sugar

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"

func StackRecord(depth int) string {
	return stack.Record(depth + 1)
}
