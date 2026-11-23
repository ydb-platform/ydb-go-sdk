package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicLegacyVectorLabels(t *testing.T)     { checkTopicVectorLabels(t, false) }
func TestTopicDescriptorVectorLabels(t *testing.T) { checkTopicVectorLabels(t, true) }
func checkTopicVectorLabels(t *testing.T, descriptors bool) {
	cache := make(map[string][]string)
	var consumer string
	var registrations int
	bind := func(path string, names []string) func(map[string]string) {
		registrations++
		if _, ok := cache[path]; !ok {
			cache[path] = names
		}

		return func(labels map[string]string) {
			t.Helper()
			want := labelNameSet(cache[path])
			require.Contains(t, want, "consumer")
			if descriptors && consumer == "" {
				delete(want, "consumer")
			}
			require.Equal(t, want, labelNameSetFromValues(labels), path)
			require.Equal(t, consumer, labels["consumer"], path)
		}
	}
	cfg := strictTopicConfig{recordingConfig: recordingConfig{details: trace.DetailsAll}, bind: bind}
	var config Config = cfg
	if descriptors {
		config = strictDescriptors{cfg}
	}
	tracer := topic(config)
	for _, info := range []trace.TopicReaderInfo{
		{Consumer: "consumer", ReaderName: "reader"}, {ReaderName: "reader"}, {},
	} {
		consumer = info.Consumer
		exerciseTopicLabels(tracer, info)
	}
	require.Len(t, cache, 8)
	require.Equal(t, 8, registrations)
}

func exerciseTopicLabels(tracer trace.Topic, readerInfo trace.TopicReaderInfo) {
	tracer.OnReaderMessagesReceived(trace.TopicReaderMessagesReceivedInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	done := tracer.OnReaderReceiveDataResponse(readerBytesInfo(readerInfo, 1))
	done(trace.TopicReaderReceiveDataResponseDoneInfo{AcceptedMessages: map[string]int{"/topic": 1}})
	tracer.OnListenerReceiveMessage(listenerBytesInfo(readerInfo, 1, nil))
	tracer.OnReaderMessagesDelivered(trace.TopicReaderMessagesDeliveredInfo{
		ReaderInfo: readerInfo, Topic: "/topic", MessagesCount: 1,
	})
	tracer.OnReaderCommitQueued(trace.TopicReaderCommitQueuedInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	tracer.OnReaderCommittedNotify(trace.TopicReaderCommittedNotifyInfo{
		ReaderInfo: readerInfo,
		Topic:      "/topic", MessagesCount: 1,
	})
	tracer.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{
		ReaderInfo:    readerInfo,
		RetryDecision: "retry", StatusCode: "UNAVAILABLE", ErrorType: "transport_error",
	})
	tracer.OnReaderLocalBufferChanged(trace.TopicReaderLocalBufferChangedInfo{
		ReaderInfo:    readerInfo,
		MessagesDelta: 1,
	})
	tracer.OnReaderCreditBalanceChanged(trace.TopicReaderCreditBalanceChangedInfo{
		ReaderInfo: readerInfo,
		BytesDelta: 1,
	})
}

type strictTopicConfig struct {
	recordingConfig

	bind func(string, []string) func(map[string]string)
}

func (c strictTopicConfig) WithSystem(system string) Config {
	c.recordingConfig = c.recordingConfig.WithSystem(system).(recordingConfig)

	return c
}

func (c strictTopicConfig) CounterVec(name string, names ...string) CounterVec {
	return strictCounterVec(c.bind(c.path(name), names))
}

func (c strictTopicConfig) GaugeVec(name string, names ...string) GaugeVec {
	return strictGaugeVec(c.bind(c.path(name), names))
}

type strictDescriptors struct{ strictTopicConfig }

func (c strictDescriptors) WithSystem(system string) Config {
	return strictDescriptors{c.strictTopicConfig.WithSystem(system).(strictTopicConfig)}
}

func (c strictDescriptors) CounterVecWithDescriptor(name, _ string, names ...string) CounterVec {
	return strictCounterVec(c.bind(name, names))
}

func (c strictDescriptors) GaugeVecWithDescriptor(name, _ string, names ...string) GaugeVec {
	return strictGaugeVec(c.bind(name, names))
}

type strictCounterVec func(map[string]string)

func (v strictCounterVec) With(labels map[string]string) Counter {
	v(labels)

	return strictCounter{}
}

type strictGaugeVec func(map[string]string)

func (v strictGaugeVec) With(labels map[string]string) Gauge {
	v(labels)

	return strictGauge{}
}

type strictCounter struct{}

func (strictCounter) Inc() {}

type strictGauge struct{}

func (strictGauge) Add(float64) {}

func (strictGauge) Set(float64) {}

func labelNameSetFromValues(labels map[string]string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for name := range labels {
		set[name] = struct{}{}
	}

	return set
}

func labelNameSet(labelNames []string) map[string]struct{} {
	set := make(map[string]struct{}, len(labelNames))
	for _, name := range labelNames {
		set[name] = struct{}{}
	}

	return set
}
