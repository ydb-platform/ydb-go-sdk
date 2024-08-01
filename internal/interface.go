package internal

// Interface is marker for internal interface
// SDK do not expect outside implementations of marked interface.
// Marked interface may be extended any time.
type Interface interface {
	privateMarkerFunc()
}

// InterfaceImplementation implemented Interface, embed the struct to internal types
type InterfaceImplementation struct{}

func (_ InterfaceImplementation) privateMarkerFunc() {}
