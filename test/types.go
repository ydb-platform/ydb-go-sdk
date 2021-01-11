package test

import "bytes"

type Embeded struct {
}

type Type struct {
	Embeded
	String  string
	Integer int
	Boolean bool
	Error   error
	Reader  bytes.Reader
}
