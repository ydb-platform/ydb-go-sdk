// +build darwin

package test

//go:generate gtrace

//gtrace:gen
type ConditionalBuildTrace struct {
	OnSomething func()
}
