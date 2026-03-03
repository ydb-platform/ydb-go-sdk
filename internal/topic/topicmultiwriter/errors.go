package topicmultiwriter

import "errors"

var (
	ErrAlreadyClosed = errors.New("multiwriter already closed")
	ErrNoSeqNo       = errors.New("seq no is required")
	ErrNoBounds      = errors.New("partition has no bounds")
)
