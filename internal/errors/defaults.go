package errors

import (
	"errors"
)

var (
	// ErrClosed is returned when operation requested on a closed driver.
	ErrClosed = errors.New("driver closed")

	// ErrNilConnection is returned when use nil preferred connection
	ErrNilConnection = errors.New("nil connection")
)
