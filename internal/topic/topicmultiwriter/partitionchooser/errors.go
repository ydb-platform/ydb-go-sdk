package partitionchooser

import "errors"

var (
	ErrNoBounds    = errors.New("no bounds configured")
	ErrUnsupported = errors.New("unsupported")
)
