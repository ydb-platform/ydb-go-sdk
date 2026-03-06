package topicmultiwriter

import "errors"

var (
	ErrAlreadyClosed                    = errors.New("multiwriter already closed")
	ErrNoSeqNo                          = errors.New("seq no is required")
	ErrNoBounds                         = errors.New("partition has no bounds")
	ErrInvalidConfiguration             = errors.New("invalid configuration")
	ErrHashPartitionChooserNotSupported = errors.New("hash partition chooser is not supported when auto partitioning is enabled")
)
