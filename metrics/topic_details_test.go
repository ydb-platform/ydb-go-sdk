package metrics

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicGaugeCloseAfterDetailsChange(t *testing.T) {
	details := trace.TopicReaderMessageEvents
	cfg := newMutableTopicConfig(&details)
	tracer := topic(cfg)
	var local, credit topicreadercommon.MetricBalance
	emitLocal := func(n int) {
		tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{MessagesDelta: n})
	}
	emitCredit := func(n int) {
		tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{BytesDelta: n})
	}
	labels := map[string]string{"endpoint": "", "database": "", "reader.name": ""}
	require.True(t, local.ChangeBuffer(7, emitLocal))
	credit.Change(128, emitCredit)
	require.Equal(t, float64(7), cfg.registry.value("ydb.topic.reader.local_buffer.messages", labels))
	require.Equal(t, float64(128), cfg.registry.value("ydb.topic.reader.credit_balance_bytes", labels))

	details = 0
	local.Finalize(emitLocal)
	credit.Finalize(emitCredit)
	assert.Zero(t, cfg.registry.value("ydb.topic.reader.local_buffer.messages", labels))
	assert.Zero(t, cfg.registry.value("ydb.topic.reader.credit_balance_bytes", labels))
}

func TestTopicReceivedBytesAndMessagesKeepInitialDetails(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		before, after           trace.Details
		wantBytes, wantMessages int64
	}{
		{"pull_to_listener", trace.TopicReaderMessageEvents, trace.TopicListenerStreamEvents, 128, 2},
		{"listener_to_pull", trace.TopicListenerStreamEvents, trace.TopicReaderMessageEvents, 256, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			details := tc.before
			cfg := newMutableTopicConfig(&details)
			tracer := topic(cfg)
			details = tc.after

			pull, listener := trace.TopicReaderInfo{}, trace.TopicReaderInfo{Listener: true}
			if tracer.OnReaderReceiveDataResponse != nil {
				tracer.OnReaderReceiveDataResponse(readerBytesInfo(pull, 128))
			}
			if tracer.OnListenerReceiveMessage != nil {
				tracer.OnListenerReceiveMessage(listenerBytesInfo(listener, 256, nil))
			}
			tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{ReaderInfo: pull, MessagesCount: 2})
			tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{ReaderInfo: listener, MessagesCount: 3})
			assert.Equal(t, []int64{tc.wantBytes}, cfg.capture.adds["ydb.topic.reader.received.bytes"])
			assert.Equal(t, []int64{tc.wantMessages}, cfg.capture.adds["ydb.topic.reader.received.messages"])
		})
	}
}

func TestTopicCommitTrackingFollowsReaderKind(t *testing.T) {
	for _, tc := range []struct {
		name           string
		details        trace.Details
		listener, want bool
	}{
		{"pull_reader_with_reader_details", trace.TopicReaderStreamEvents, false, true},
		{"pull_reader_with_listener_details", trace.TopicListenerStreamEvents, false, false},
		{"listener_with_reader_details", trace.TopicReaderStreamEvents, true, false},
		{"listener_with_listener_details", trace.TopicListenerStreamEvents, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			details := tc.details
			tracer := topic(newMutableTopicConfig(&details))
			session := topicreadercommon.NewPartitionSession(context.Background(), "topic", 1, 1, "", 2, 3, 0)
			defer session.Close()
			session.SetupCommitMetrics(&tracer, trace.TopicReaderInfo{Listener: tc.listener})
			commitRange := topicreadercommon.CommitRange{
				PartitionSession: session, CommitOffsetStart: 7, CommitOffsetEnd: 8,
			}
			assert.Equal(t, tc.want, topicreadercommon.RegisterCommitQueued(commitRange) == 1, "range admitted")
			assert.Equal(t, tc.want, topicreadercommon.RegisterCommitAcknowledged(session, 8) == 1, "span tracked")
		})
	}
}

type mutableTopicConfig struct {
	topicAddCounterConfig

	mask *trace.Details
}

func newMutableTopicConfig(mask *trace.Details) mutableTopicConfig {
	return mutableTopicConfig{
		topicAddCounterConfig: topicAddCounterConfig{
			recordingConfig: recordingConfig{registry: newRecordingRegistry()},
			capture:         &topicCounterCapture{},
		},
		mask: mask,
	}
}

func (c mutableTopicConfig) Details() trace.Details { return *c.mask }

func (c mutableTopicConfig) WithSystem(system string) Config {
	c.topicAddCounterConfig = c.topicAddCounterConfig.WithSystem(system).(topicAddCounterConfig)

	return c
}
