package value

import "errors"

var (
	ErrCannotCast                   = errors.New("cast failed")
	errDestinationTypeIsNotAPointer = errors.New("destination type is not a pointer")
	errNilDestination               = errors.New("destination is nil")
)
