package scanner

import (
	"errors"
)

var (
	errColumnsNotFoundInRow               = errors.New("some columns not found in row")
	errFieldsNotFoundInStruct             = errors.New("some fields not found in struct")
	errIncompatibleColumnsAndDestinations = errors.New("incompatible columns and destinations")
	errDstTypeIsNotAPointer               = errors.New("dst type is not a pointer")
	errDstTypeIsNotAPointerToStruct       = errors.New("dst type is not a pointer to struct")
)
