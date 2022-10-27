package structural

import "time"

type Logger interface {
	// Record produces new Record.
	// The record should be completely empty (i.e. no level or fields).
	Record() Record
	WithName(name string) Logger
}

// Skipper is an optional interface for Loggers to implement.
// It is useful for implemetations with stack trace support.
type Skipper interface {
	// WithCallerSkip sets the number of callers to skip in stack trace.
	// WithCallerSkip is not used when logging (e.g. in traces), so Logger
	// implementations should add caller skipping for Record.Message themselves.
	// WithCallerSkip is used in Logger wrappers such as WithPool.
	WithCallerSkip(n int) Logger
}

type Record interface {
	// Level sets Record's logging level.
	// If called multiple times, the most recent level should be used.
	Level(lvl Level) Record

	String(key string, value string) Record
	Strings(key string, value []string) Record
	Duration(key string, value time.Duration) Record
	Error(value error) Record

	// Reset clears Record level and fields.
	// After Reset, the Record should be as if it was just returned from Logger.Record().
	Reset()

	// Message sends Record with the message specified.
	Message(msg string)
}
