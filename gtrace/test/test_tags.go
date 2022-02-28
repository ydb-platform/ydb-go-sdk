package test

//go:generate gtrace -v -tag gtrace

//gtrace:gen
//gtrace:set context
//gtrace:set shortcut
type BuildTagTrace struct {
	OnSomethingA func() func()
	OnSomethingB func(int8, int16) func(int32, int64)
	OnSomethingC func(Type) func(Type)
}
