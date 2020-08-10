package test

//go:generate gtrace -v

//gtrace:gen
//gtrace:set context
//gtrace:set shortcut
type Trace struct {
	OnTest func(string) func(string)

	OnAction     func(TraceActionStart) func(TraceActionDone)
	OnActionPtr  func(*TraceActionStart) func(*TraceActionDone)
	OnSomething0 func(int8) func(int16) func(int32) func(int64)
	OnSomething1 func(int8, int16) func(int32, int64)
	OnSomething2 func(Type) func(Type) func(Type)
	OnAnother    func()

	// Not supported signatures:
	Skipped0 func() string
	Skipped1 func() (func(), func())
}

type TraceActionStart struct {
	String string
	Nested Type
}

type TraceActionDone struct {
	Error error
}
