package gtrace

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

// ComposeReader preserves the queued-commit opt-in: a logging-only reader must
// not retain commit ranges merely because its traces were composed.
func ComposeReader(lhs, rhs *trace.Topic, opts ...TopicComposeOption) *trace.Topic {
	if lhs == nil {
		return rhs
	}
	t := Compose(lhs, rhs, opts...)
	if lhs.OnReaderCommitQueued == nil && rhs.OnReaderCommitQueued == nil {
		t.OnReaderCommitQueued = nil
	}

	return t
}
