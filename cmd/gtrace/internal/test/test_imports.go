package test

import (
	internal "github.com/ydb-platform/ydb-go-sdk/v3/cmd/gtrace/internal/types"
)

//go:generate gtrace -v

//gtrace:gen
//gtrace:set context
// NOTE: must compile without unused imports error.
type TraceNoShortcut struct {
	OnSomethingA func(Type)
	OnSomethingB func(internal.Type)
}
