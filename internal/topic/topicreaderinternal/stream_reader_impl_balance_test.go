package topicreaderinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderBalanceWithoutObservers(t *testing.T) {
	tracer := gtrace.ComposeReader(&trace.Topic{}, &trace.Topic{})
	require.Nil(t, tracer.OnReaderCreditBalanceChanged)
	require.Nil(t, tracer.OnReaderLocalBufferChanged)
}

func BenchmarkReaderBalanceWithoutObservers(b *testing.B) {
	reader := topicStreamReaderImpl{cfg: newTopicStreamReaderConfig()}
	reader.cfg.Trace = gtrace.ComposeReader(&trace.Topic{}, &trace.Topic{})
	b.ReportAllocs()
	for b.Loop() {
		reader.reserveLocalBuffer(1)
		reader.releaseLocalBuffer(1)
		reader.changeCreditBalance(1024)
		reader.changeCreditBalance(-1024)
	}
}
