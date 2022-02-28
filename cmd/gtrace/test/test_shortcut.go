package test

//go:generate gtrace -v

//gtrace:gen
type ShortcutPerFieldTrace struct {
	//gtrace:set shortcut
	OnFoo func()
	OnBar func()
}
