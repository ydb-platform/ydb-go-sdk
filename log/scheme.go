package log

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Scheme returns trace.Scheme with logging events from details
func Scheme(l Logger, d trace.Detailer, opts ...Option) trace.Scheme {
	return trace.Scheme{}
}
