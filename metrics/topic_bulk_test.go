package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicBulkCountersUseFloatAdd(t *testing.T) {
	cfg := floatCounterConfig{
		recordingConfig: recordingConfig{registry: newRecordingRegistry(), details: trace.DetailsAll},
		counters:        make(map[string]*floatCounter),
	}
	tracer := topic(cfg)
	done := tracer.OnReaderReceiveDataResponse(readerBytesInfo(trace.TopicReaderInfo{}, 128))
	done(trace.TopicReaderReceiveDataResponseDoneInfo{AcceptedMessages: map[string]int{"topic": 5}})
	tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{MessagesCount: 4})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{MessagesCount: 3})
	tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{MessagesCount: 2})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{})
	for name, delta := range map[string]float64{
		"received.messages": 5, "delivered.messages": 4, "received.bytes": 128,
		"commit.queued": 3, "commit.acknowledged": 2,
	} {
		c := cfg.counters["topic.reader."+name]
		require.Equal(t, []float64{delta}, c.adds, name)
		require.Zero(t, c.incs, name)
	}
	errors := cfg.counters["topic.reader.session.errors"]
	require.Equal(t, 1, errors.incs)
	require.Empty(t, errors.adds)
}

type floatCounterConfig struct {
	recordingConfig

	counters map[string]*floatCounter
}

func (c floatCounterConfig) WithSystem(system string) Config {
	c.recordingConfig = c.recordingConfig.WithSystem(system).(recordingConfig)

	return c
}

func (c floatCounterConfig) CounterVec(name string, _ ...string) CounterVec {
	v := &floatCounter{}
	c.counters[c.path(name)] = v

	return v
}

type floatCounter struct {
	adds []float64
	incs int
}

func (c *floatCounter) With(map[string]string) Counter { return c }
func (c *floatCounter) Add(n float64)                  { c.adds = append(c.adds, n) }
func (c *floatCounter) Inc()                           { c.incs++ }
