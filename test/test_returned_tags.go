package test

//go:generate gtrace -v -tag gtrace

//gtrace:gen
//gtrace:set shortcut
type TraceReturningTraceTags struct {
	OnReturnedTrace func() ReturnedTrace
}
