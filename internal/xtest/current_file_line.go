package xtest

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"

func CurrentFileLine() string {
	return stack.Record(1,
		stack.PackagePath(false),
		stack.PackageName(false),
		stack.StructName(false),
		stack.FunctionName(false),
		stack.Lambda(false),
	)
}
