package config

import (
	"time"
)

type Common struct {
	operationTimeout     time.Duration
	operationCancelAfter time.Duration
	disableAutoRetry     bool

	panicCallback func(e interface{})
}

// AutoRetry defines auto-retry flag
func (c *Common) AutoRetry() bool {
	return !c.disableAutoRetry
}

// PanicCallback returns user-defined panic callback
// If nil - panic callback not defined
func (c *Common) PanicCallback() func(e interface{}) {
	return c.panicCallback
}

// OperationTimeout is the maximum amount of time a YDB server will process
// an operation. After timeout exceeds YDB will try to cancel operation and
// regardless of the cancellation appropriate error will be returned to
// the client.
// If OperationTimeout is zero then no timeout is used.
func (c *Common) OperationTimeout() time.Duration {
	return c.operationTimeout
}

// OperationCancelAfter is the maximum amount of time a YDB server will process an
// operation. After timeout exceeds YDB will try to cancel operation and if
// it succeeds appropriate error will be returned to the client; otherwise
// processing will be continued.
// If OperationCancelAfter is zero then no timeout is used.
func (c *Common) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

// SetOperationTimeout define the maximum amount of time a YDB server will process
// an operation. After timeout exceeds YDB will try to cancel operation and
// regardless of the cancellation appropriate error will be returned to
// the client.
//
// If OperationTimeout is zero then no timeout is used.
func SetOperationTimeout(c *Common, operationTimeout time.Duration) {
	c.operationTimeout = operationTimeout
}

// SetOperationCancelAfter set the maximum amount of time a YDB server will process an
// operation. After timeout exceeds YDB will try to cancel operation and if
// it succeeds appropriate error will be returned to the client; otherwise
// processing will be continued.
//
// If OperationCancelAfter is zero then no timeout is used.
func SetOperationCancelAfter(c *Common, operationCancelAfter time.Duration) {
	c.operationCancelAfter = operationCancelAfter
}

// SetPanicCallback applies panic callback to config
func SetPanicCallback(c *Common, panicCallback func(e interface{})) {
	c.panicCallback = panicCallback
}

// SetAutoRetry affects on AutoRetry() flag
func SetAutoRetry(c *Common, autoRetry bool) {
	c.disableAutoRetry = !autoRetry
}
