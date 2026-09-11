package topicreaderinternal

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicStreamReaderLocalBufferEventsKeepOrderDuringClose(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	reader := e.reader
	var observations []int
	balance := 0
	reader.cfg.Trace = gtrace.Compose(&trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			if info.MessagesDelta > 0 {
				reader.finalizeLocalBuffer()
			}
		},
	}, &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			balance += info.MessagesDelta
			observations = append(observations, balance)
		},
	})
	reader.reserveLocalBuffer("topic", 1)
	require.Equal(t, []int{1, 0}, observations)
}

func TestTopicStreamReaderCreditEventsKeepOrderDuringClose(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	reader := e.reader
	var observations []int
	balance := 0
	reader.cfg.Trace = gtrace.Compose(&trace.Topic{
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			if info.BytesDelta > 0 {
				reader.finalizeCreditBalance()
			}
		},
	}, &trace.Topic{
		OnReaderCreditBalanceChanged: func(info trace.TopicReaderCreditBalanceChangedInfo) {
			balance += info.BytesDelta
			observations = append(observations, balance)
		},
	})
	reader.changeCreditBalance(100)
	require.Equal(t, []int{100, 0}, observations)
}
