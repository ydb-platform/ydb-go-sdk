//go:build windows
// +build windows

package test

//go:generate gtrace -v

//gtrace:gen
type ConditionalBuildTrace struct {
	OnSomething func()
}
