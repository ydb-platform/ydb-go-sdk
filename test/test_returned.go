package test

//go:generate gtrace -v -tag debug

//gtrace:gen
//gtrace:set shortcut
type TraceReturningTrace struct {
	OnReturnedTrace func() ReturnedTrace
}

//gtrace:gen
type ReturnedTrace struct {
	OnSomething func(a, b int)

	OnFoo func(_ int, _ int)
	OnBar func(_, _ int)
	OnBaz func(int, int)
}
