package config

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Config is an configuration of coordination client
//
//nolint:maligned
type Config struct {
	config.Common

	trace trace.Coordination
}

// Trace returns trace over coordination client calls
func (c Config) Trace() trace.Coordination {
	return c.trace
}

type Option func(c *Config)

// WithTrace appends coordination trace to early defined traces
func WithTrace(trace trace.Coordination, opts ...trace.CoordinationComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(trace, opts...)
	}
}

// With applies common configuration params
func With(config config.Common) Option {
	return func(c *Config) {
		c.Common = config
	}
}

func New(opts ...Option) Config {
	c := Config{}
	for _, o := range opts {
		o(&c)
	}
	return c
}

type SessionStartConfig struct {
	sessionID     uint64
	timeoutMillis uint64
}

func NewSessionStartConfig() *SessionStartConfig {
	return &SessionStartConfig{
		sessionID:     0,
		timeoutMillis: 500000,
	}
}

func (s *SessionStartConfig) SessionID() uint64 {
	return s.sessionID
}

func (s *SessionStartConfig) TimeoutMillis() uint64 {
	return s.timeoutMillis
}

type SessionStartOption func(s *SessionStartConfig)

func WithSessionTimeoutMillis(timeoutMillis uint64) SessionStartOption {
	return func(s *SessionStartConfig) {
		s.timeoutMillis = timeoutMillis
	}
}

func WithSessionID(sessionID uint64) SessionStartOption {
	return func(s *SessionStartConfig) {
		s.sessionID = sessionID
	}
}
