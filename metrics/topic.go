package metrics

import (
	"strings"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	topicMessageLabels      = []string{endpointLabel, databaseLabel, "topic", consumerLabel, readerNameLabel}
	topicStreamLabels       = []string{endpointLabel, databaseLabel, consumerLabel, readerNameLabel}
	topicSessionErrorLabels = []string{
		endpointLabel, databaseLabel, consumerLabel, readerNameLabel,
		"retry_decision", "status_code", "error.type",
	}
)

type topicBinding[T any] func(map[string]string) T

type topicVectors struct {
	received, delivered, bytes, errors, queued, acknowledged topicBinding[Counter]
	local, credit                                            topicBinding[Gauge]
}

func registerTopicVectors(reader Config) (v topicVectors) {
	const messageUnit = "{message}"
	const byteUnit = "By"
	for _, metric := range []struct {
		name, unit string
		labels     []string
		counter    *topicBinding[Counter]
		gauge      *topicBinding[Gauge]
	}{
		{"ydb.topic.reader.received.messages", messageUnit, topicMessageLabels, &v.received, nil},
		{"ydb.topic.reader.delivered.messages", messageUnit, topicMessageLabels, &v.delivered, nil},
		{"ydb.topic.reader.received.bytes", byteUnit, topicStreamLabels, &v.bytes, nil},
		{"ydb.topic.reader.session.errors", "{error}", topicSessionErrorLabels, &v.errors, nil},
		{"ydb.topic.reader.commit.queued", messageUnit, topicMessageLabels, &v.queued, nil},
		{"ydb.topic.reader.commit.acknowledged", messageUnit, topicMessageLabels, &v.acknowledged, nil},
		{"ydb.topic.reader.local_buffer.messages", messageUnit, topicStreamLabels, nil, &v.local},
		{"ydb.topic.reader.credit_balance_bytes", byteUnit, topicStreamLabels, nil, &v.credit},
	} {
		path := strings.TrimPrefix(metric.name, "ydb.topic.reader.")
		system, name, ok := strings.Cut(path, ".")
		if !ok {
			system, name = "", path
		}
		if metric.counter != nil {
			*metric.counter = topicCounter(reader, system, name, metric.name, metric.unit, metric.labels)
		} else {
			*metric.gauge = topicGauge(reader, system, name, metric.name, metric.unit, metric.labels)
		}
	}

	return v
}

func topic(config Config) (t trace.Topic) {
	reader := config.WithSystem("topic").WithSystem("reader")
	vectors := registerTopicVectors(reader)
	messageEnabled := func(info trace.TopicReaderInfo) bool {
		return topicMetricEnabled(reader.Details(), info.Listener, trace.TopicReaderMessageEvents)
	}
	if reader.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) != 0 {
		t.OnReaderMessagesReceived = func(info trace.TopicReaderMessagesReceivedInfo) {
			if messageEnabled(info.ReaderInfo) {
				addCounter(vectors.received(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
			}
		}
		t.OnReaderMessagesDelivered = func(info trace.TopicReaderMessagesDeliveredInfo) {
			if messageEnabled(info.ReaderInfo) {
				addCounter(vectors.delivered(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
			}
		}
		t.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
			if messageEnabled(info.ReaderInfo) {
				addGauge(vectors.credit(streamLabels(info.ReaderInfo)), float64(info.BytesDelta))
			}
		}
		t.OnReaderLocalBufferChanged = func(info trace.TopicReaderLocalBufferChangedInfo) {
			if messageEnabled(info.ReaderInfo) {
				addGauge(vectors.local(streamLabels(info.ReaderInfo)), float64(info.MessagesDelta))
			}
		}
	}

	streamEnabled := func(info trace.TopicReaderInfo) bool {
		return topicMetricEnabled(reader.Details(), info.Listener, trace.TopicReaderStreamEvents)
	}
	if reader.Details()&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) != 0 {
		t.OnReaderCommitQueued = func(info trace.TopicReaderCommitQueuedInfo) {
			if streamEnabled(info.ReaderInfo) {
				addCounter(vectors.queued(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
			}
		}
		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			if streamEnabled(info.ReaderInfo) && info.MessagesCount > 0 {
				labels := messageLabels(info.ReaderInfo, info.Topic)
				addCounter(vectors.acknowledged(labels), info.MessagesCount)
			}
		}
		t.OnReaderSessionError = func(info trace.TopicReaderSessionErrorInfo) {
			if !streamEnabled(info.ReaderInfo) {
				return
			}
			labels := streamLabels(info.ReaderInfo)
			labels["retry_decision"] = info.RetryDecision
			labels["status_code"] = info.StatusCode
			labels["error.type"] = info.ErrorType
			vectors.errors(labels).Inc()
		}
	}
	setReceivedBytesHooks(&t, reader.Details(), vectors)

	return t
}

func setReceivedBytesHooks(t *trace.Topic, details trace.Details, vectors topicVectors) {
	recordBytes := func(info trace.TopicReaderInfo, count int) {
		if count > 0 {
			addCounter(vectors.bytes(streamLabels(info)), count)
		}
	}
	if details&trace.TopicReaderMessageEvents != 0 {
		t.OnReaderReceiveDataResponse = func(
			info trace.TopicReaderReceiveDataResponseStartInfo,
		) func(trace.TopicReaderReceiveDataResponseDoneInfo) {
			if info.DataResponse != nil {
				recordBytes(info.ReaderInfo, info.DataResponse.GetBytesSize())
			}

			return nil
		}
	}

	if details&trace.TopicListenerStreamEvents != 0 {
		t.OnListenerReceiveMessage = func(info trace.TopicListenerReceiveMessageInfo) {
			if info.Error != nil {
				return
			}
			recordBytes(info.ReaderInfo, info.BytesSize)
		}
	}
}

func topicCounter(
	readerConfig Config, legacySystem, legacyName, name, unit string, labels []string,
) topicBinding[Counter] {
	if registry, ok := readerConfig.(RegistryWithDescriptors); ok {
		return descriptorBinding(registry.CounterVecWithDescriptor(name, unit, labels...).With)
	}

	return readerConfig.WithSystem(legacySystem).CounterVec(legacyName, labels...).With
}

func topicGauge(
	readerConfig Config, legacySystem, legacyName, name, unit string, labels []string,
) topicBinding[Gauge] {
	if registry, ok := readerConfig.(RegistryWithGaugeDescriptors); ok {
		return descriptorBinding(registry.GaugeVecWithDescriptor(name, unit, labels...).With)
	}

	if legacySystem == "" {
		return readerConfig.GaugeVec(legacyName, labels...).With
	}

	return readerConfig.WithSystem(legacySystem).GaugeVec(legacyName, labels...).With
}

func descriptorBinding[T any](bind topicBinding[T]) topicBinding[T] {
	return func(labels map[string]string) T {
		if labels[consumerLabel] == "" {
			delete(labels, consumerLabel)
		}

		return bind(labels)
	}
}

func messageLabels(readerInfo trace.TopicReaderInfo, topic string) map[string]string {
	labels := streamLabels(readerInfo)
	labels["topic"] = topic

	return labels
}

func streamLabels(readerInfo trace.TopicReaderInfo) map[string]string {
	return map[string]string{
		consumerLabel:   readerInfo.Consumer,
		endpointLabel:   readerInfo.Endpoint,
		databaseLabel:   readerInfo.Database,
		readerNameLabel: readerInfo.ReaderName,
	}
}

func addCounter(counter Counter, delta int) {
	if delta <= 0 {
		return
	}
	if adder, ok := counter.(interface{ Add(delta int64) }); ok {
		adder.Add(int64(delta))

		return
	}
	if adder, ok := counter.(interface{ Add(delta float64) }); ok {
		adder.Add(float64(delta))
	}
}

func addGauge(gauge Gauge, delta float64) {
	if delta == 0 {
		return
	}
	gauge.Add(delta)
}

// Listener stream events carry the shared reader metrics for the listener API.
func topicMetricEnabled(details trace.Details, listener bool, readerEvents trace.Details) bool {
	if listener {
		return details&trace.TopicListenerStreamEvents != 0
	}

	return details&readerEvents != 0
}
