package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// TopicOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type TopicOption func(c *topic.Config)

// WithTrace defines trace over persqueue client calls
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithTrace(trace trace.Topic, opts ...trace.TopicComposeOption) TopicOption { //nolint:gocritic
	return func(c *topic.Config) {
		c.Trace = c.Trace.Compose(&trace, opts...)
	}
}

// WithOperationTimeout set the maximum amount of time a YDB server will process
// an operation. After timeout exceeds YDB will try to cancel operation and
// regardless of the cancellation appropriate error will be returned to
// the client.
// If OperationTimeout is zero then no timeout is used.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithOperationTimeout(operationTimeout time.Duration) TopicOption {
	return func(c *topic.Config) {
		config.SetOperationTimeout(&c.Common, operationTimeout)
	}
}

// WithOperationCancelAfter set the maximum amount of time a YDB server will process an
// operation. After timeout exceeds YDB will try to cancel operation and if
// it succeeds appropriate error will be returned to the client; otherwise
// processing will be continued.
// If OperationCancelAfter is zero then no timeout is used.
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithOperationCancelAfter(operationCancelAfter time.Duration) TopicOption {
	return func(c *topic.Config) {
		config.SetOperationCancelAfter(&c.Common, operationCancelAfter)
	}
}
