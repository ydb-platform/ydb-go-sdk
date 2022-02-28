package closer

import (
	"context"
)

// Closer is the interface that wraps the basic Close method.
type Closer interface {
	// Close closes table client
	Close(ctx context.Context) error
}
