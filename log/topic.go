package log

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

// Topic returns trace.Topic with logging events from details
func Topic(l Logger, details trace.Details) (t trace.Topic) {
	if details&trace.TopicEvents == 0 {
		return
	}
	_ = l.WithName(`topic`)
	return t
}
