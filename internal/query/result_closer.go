package query

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

var errResultCloserNilReason = io.EOF

// ResultCloser provides a mechanism to close query results and handle cleanup operations.
// It tracks the reason for closing, exposes a closed flag for synchronization,
// and allows registering cleanup functions to be called on close.
type ResultCloser struct {
	reason  error
	closed  atomic.Bool
	mu      sync.Mutex
	onClose []func()
}

// NewResultCloser creates and returns a new ResultCloser instance.
func NewResultCloser() *ResultCloser {
	return &ResultCloser{}
}

// Close closes the ResultCloser with the specified reason error.
// If the ResultCloser is already closed, this method does nothing.
// If reason is nil, it will be set to io.EOF.
// All registered onClose functions will be called in LIFO order.
// After calling Close, Closed returns true to signal completion.
func (r *ResultCloser) Close(reason error) {
	if r.doneWithReason(reason) { // only first [r.Close] invoke runs callbacks
		r.runOnCloseCallbacks()
	}
}

// doneWithReason sets the closure reason and signals completion.
// The method is idempotent - subsequent calls after the first successful call are no-ops.
// If reason is nil, it defaults to errResultCloserNilReason.
// The method uses mutex synchronization to ensure safe concurrent access.
// Returns true if the close operation was performed (first call), false otherwise.
func (r *ResultCloser) doneWithReason(reason error) (realClose bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.reason != nil {
		return false
	}

	if reason == nil {
		reason = errResultCloserNilReason
	}

	r.reason = reason
	r.closed.Store(true)

	return true
}

// runOnCloseCallbacks executes registered cleanup callbacks in reverse order (LIFO).
// This method is NOT safe for concurrent access.
func (r *ResultCloser) runOnCloseCallbacks() {
	for i := range r.onClose { // descending calls for LIFO
		r.onClose[len(r.onClose)-i-1]()
	}
}

// Err returns the reason error that was passed to Close.
// If Close has not been called yet, it returns nil.
func (r *ResultCloser) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.reason
}

// Closed reports whether Close has completed (including a concurrent first Close).
func (r *ResultCloser) Closed() bool {
	return r.closed.Load()
}

// doneErr reports whether the closer has finished and returns its reason.
// err is never nil when done is true (nil reason is mapped to io.EOF).
// Prefer this over separate Closed()+Err() calls: those can observe a transient
// closed flag before reason is visible and yield err == nil.
func (r *ResultCloser) doneErr() (done bool, err error) {
	if !r.closed.Load() {
		return false, nil
	}

	r.mu.Lock()
	err = r.reason
	r.mu.Unlock()

	if err == nil {
		return true, errResultCloserNilReason
	}

	return true, err
}

// CloseOnContextCancel registers a callback function that closes the ResultCloser
// when the provided context is cancelled.
// It returns a function that can be used to stop the callback.
func (r *ResultCloser) CloseOnContextCancel(ctx context.Context) func() bool {
	return context.AfterFunc(ctx, func() {
		r.Close(ctx.Err())
	})
}

// OnClose registers a function to be called when the ResultCloser is closed.
// Multiple functions can be registered and they will be called in LIFO order
// when Close is called.
func (r *ResultCloser) OnClose(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.onClose = append(r.onClose, f)
}
