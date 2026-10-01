package transactionalwriterbenchmark

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestInstrumentationCountsWriterStreamInitialization(t *testing.T) {
	t.Parallel()

	metrics := &instrumentation{}
	start := metrics.topicTrace().OnWriterInitStream
	start(trace.TopicWriterInitStreamStartInfo{})
	if got := metrics.streamWriteOpens.Load(); got != 1 {
		t.Fatalf("streamWriteOpens = %d, want 1", got)
	}
}
