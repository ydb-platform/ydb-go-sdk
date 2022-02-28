package test

import "bytes"

type Embedded struct{}

type Type struct {
	Embedded
	String  string
	Integer int
	Boolean bool
	Error   error
	Reader  bytes.Reader
}
