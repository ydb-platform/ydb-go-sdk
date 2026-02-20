package topicproducer

import "errors"

var (
	ErrAlreadyClosed = errors.New("producer already closed")
	ErrNoSeqNo       = errors.New("seq no is required")
)
