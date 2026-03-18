package topicmultiwriter

import "errors"

var (
	ErrAlreadyClosed                    = errors.New("multiwriter already closed")
	ErrNoSeqNo                          = errors.New("seq no is required")
	ErrInvalidConfiguration             = errors.New("invalid configuration")
	ErrNotImplemented                   = errors.New("not implemented")
	ErrHashPartitionChooserNotSupported = errors.New("hash partition chooser is not supported when auto partitioning is enabled")                  //nolint:lll
	ErrNoBounds                         = errors.New("no bounds assigned to partitions, turn on auto partitioning to use bound partition chooser") //nolint:lll
)
