package xsync

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
)

// UnboundedChan is a generic unbounded channel implementation that supports
// message merging and concurrent access.
type UnboundedChan[T any] struct {
	// signal is a capacity-1 wakeup for Receive only; it is never closed.
	// Closing it would race with Send/SendWithMerge after Close (e.g. TopicListener
	// shutdown) and panic with "send on closed channel", because notify sends
	// outside the buffer mutex. Shutdown is signaled via closed and notify().
	signal empty.Chan

	mutex  Mutex
	buffer []T
	closed bool
}

// NewUnboundedChan creates a new UnboundedChan instance.
func NewUnboundedChan[T any]() *UnboundedChan[T] {
	return &UnboundedChan[T]{
		signal: make(empty.Chan, 1),
		buffer: make([]T, 0),
	}
}

func (c *UnboundedChan[T]) notify() {
	select {
	case c.signal <- empty.Struct{}:
	default:
	}
}

// Send adds a message to the channel.
// The operation is non-blocking and thread-safe.
func (c *UnboundedChan[T]) Send(msg T) {
	var notify bool
	c.mutex.WithLock(func() {
		if !c.closed {
			c.buffer = append(c.buffer, msg)
			notify = true
		}
	})

	if notify {
		c.notify()
	}
}

// SendWithMerge adds a message to the channel with optional merging.
// If mergeFunc returns true, the new message will be merged with the last message.
// The merge operation is atomic and preserves message order.
func (c *UnboundedChan[T]) SendWithMerge(msg T, mergeFunc func(last, new T) (T, bool)) {
	var notify bool
	c.mutex.WithLock(func() {
		if c.closed {
			return
		}

		if len(c.buffer) > 0 {
			if merged, shouldMerge := mergeFunc(c.buffer[len(c.buffer)-1], msg); shouldMerge {
				c.buffer[len(c.buffer)-1] = merged
				notify = true

				return
			}
		}

		c.buffer = append(c.buffer, msg)
		notify = true
	})

	if notify {
		c.notify()
	}
}

// Receive retrieves a message from the channel with context support.
// Returns (message, true, nil) if a message is available.
// Returns (zero_value, false, nil) if the channel is closed and empty.
// Returns (zero_value, false, context.Canceled) if context is cancelled.
// Returns (zero_value, false, context.DeadlineExceeded) if context times out.
func (c *UnboundedChan[T]) Receive(ctx context.Context) (T, bool, error) {
	for {
		var msg T
		var hasMsg, isClosed bool

		c.mutex.WithLock(func() {
			if len(c.buffer) > 0 {
				msg = c.buffer[0]
				c.buffer = c.buffer[1:]
				hasMsg = true
			}
			isClosed = c.closed
		})

		if hasMsg {
			return msg, true, nil
		}
		if isClosed {
			return msg, false, nil
		}

		// Wait for signal that something happened or context cancellation
		select {
		case <-ctx.Done():
			return msg, false, ctx.Err()
		case <-c.signal:
			// Loop back to check state again
		}
	}
}

// DrainBuffered removes and returns all currently buffered messages without blocking.
func (c *UnboundedChan[T]) DrainBuffered() []T {
	var drained []T

	c.mutex.WithLock(func() {
		if len(c.buffer) == 0 {
			return
		}

		drained = c.buffer
		c.buffer = make([]T, 0, cap(drained))
	})

	return drained
}

// Close closes the channel.
// After closing, Send and SendWithMerge operations will be ignored,
// and Receive will return (zero_value, false) once the buffer is empty.
func (c *UnboundedChan[T]) Close() {
	var isClosed bool
	c.mutex.WithLock(func() {
		if !c.closed {
			c.closed = true
			isClosed = true
		}
	})

	if !isClosed {
		return
	}

	c.notify()
}
