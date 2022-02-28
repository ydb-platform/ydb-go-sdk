//go:build darwin && arm64
// +build darwin,arm64

package test

//go:generate gtrace -v

//gtrace:gen
type ConditionalBuildTrace struct {
	OnSomething func()
}
