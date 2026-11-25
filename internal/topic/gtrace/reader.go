package gtrace

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

// ComposeReader preserves absent hooks for opt-in reader bookkeeping.
func ComposeReader(lhs, rhs *trace.Topic, opts ...TopicComposeOption) *trace.Topic {
	if lhs == nil {
		return rhs
	}
	t := Compose(lhs, rhs, opts...)
	if lhs.OnReaderCommitQueued == nil && rhs.OnReaderCommitQueued == nil {
		t.OnReaderCommitQueued = nil
	}
	if lhs.OnListenerCommitQueued == nil && rhs.OnListenerCommitQueued == nil {
		t.OnListenerCommitQueued = nil
	}
	if lhs.OnReaderCreditBalanceChanged == nil && rhs.OnReaderCreditBalanceChanged == nil {
		t.OnReaderCreditBalanceChanged = nil
	}
	if lhs.OnReaderLocalBufferChanged == nil && rhs.OnReaderLocalBufferChanged == nil {
		t.OnReaderLocalBufferChanged = nil
	}
	if lhs.OnReaderMessagesReceived == nil && rhs.OnReaderMessagesReceived == nil {
		t.OnReaderMessagesReceived = nil
	}

	return t
}
