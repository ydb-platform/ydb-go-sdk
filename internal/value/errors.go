package value

import "errors"

var (
	ErrCannotCast                   = errors.New("cannot cast")
	errDestinationTypeIsNotAPointer = errors.New("destination type is not a pointer")
)
