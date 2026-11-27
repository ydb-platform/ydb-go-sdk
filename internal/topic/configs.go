package topic

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config struct {
	config.Common

	Trace              *trace.Topic
	Endpoint           string
	Database           string
	MaxGrpcMessageSize int
}

type Option func(c *Config)

func PublicWithTrace(t trace.Topic) Option { //nolint:gocritic
	return func(c *Config) {
		c.Trace = c.ComposeTrace(c.Trace, &t)
	}
}

func (c *Config) ComposeTrace(base, extra *trace.Topic) *trace.Topic {
	if extra == nil {
		return base
	}
	var opts []gtrace.TopicComposeOption
	if cb := c.PanicCallback(); cb != nil {
		opts = append(opts, gtrace.WithTopicPanicCallback(cb))
	}

	return gtrace.ComposeReader(base, extra, opts...)
}

func PublicWithOperationTimeout(operationTimeout time.Duration) Option {
	return func(c *Config) {
		config.SetOperationTimeout(&c.Common, operationTimeout)
	}
}

func PublicWithOperationCancelAfter(operationCancelAfter time.Duration) Option {
	return func(c *Config) {
		config.SetOperationCancelAfter(&c.Common, operationCancelAfter)
	}
}

func WithGrpcMessageSize(sizeBytes int) Option {
	return func(c *Config) {
		c.MaxGrpcMessageSize = sizeBytes
	}
}

func WithDatabase(database string) Option {
	return func(c *Config) {
		c.Database = database
	}
}

func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.Endpoint = endpoint
	}
}
