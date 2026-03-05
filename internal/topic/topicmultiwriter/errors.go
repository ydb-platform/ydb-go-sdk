package topicmultiwriter

import "errors"

var (
	ErrAlreadyClosed        = errors.New("multiwriter already closed")
	ErrNoSeqNo              = errors.New("seq no is required")
	ErrSeqNoAlreadySet      = errors.New("seq no already set")
	ErrNoBounds             = errors.New("partition has no bounds")
	ErrInvalidConfiguration = errors.New("invalid configuration")
)
