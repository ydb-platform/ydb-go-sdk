package test

import "github.com/gobwas/gtrace/test/internal"

//go:generate gtrace -v

//gtrace:gen
//gtrace:set context
// NOTE: must compile without unused imports error.
type TraceNoShortcut struct {
	OnSomethingA func(Type)
	OnSomethingB func(internal.Type)
}
