package transactionalwriterbenchmark

import (
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type instrumentation struct {
	streamWriteOpens atomic.Uint64
}

func (m *instrumentation) topicTrace() trace.Topic {
	return trace.Topic{
		OnWriterInitStream: func(trace.TopicWriterInitStreamStartInfo) func(trace.TopicWriterInitStreamDoneInfo) {
			m.streamWriteOpens.Add(1)

			return nil
		},
	}
}
