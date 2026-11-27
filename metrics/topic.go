package metrics

import (
	"strings"

	internalConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
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

func registerTopicVectors(reader Config) topicVectors {
	const messageUnit = "{message}"
	const byteUnit = "By"
	counter := func(scope, name, unit string, labels []string) topicBinding[Counter] {
		if registry, ok := reader.(RegistryWithDescriptors); ok {
			return descriptorBinding(registry.CounterVecWithDescriptor(topicMetricName(scope, name), unit, labels...).With)
		}

		return legacyTopicBinding(reader.WithSystem(scope).CounterVec(name, legacyTopicLabelNames(labels)...).With)
	}
	gauge := func(scope, name, unit string, labels []string) topicBinding[Gauge] {
		if registry, ok := reader.(RegistryWithGaugeDescriptors); ok {
			return descriptorBinding(registry.GaugeVecWithDescriptor(topicMetricName(scope, name), unit, labels...).With)
		}
		config := reader
		if scope != "" {
			config = reader.WithSystem(scope)
		}

		return legacyTopicBinding(config.GaugeVec(name, legacyTopicLabelNames(labels)...).With)
	}

	return topicVectors{
		received:     counter("received", "messages", messageUnit, topicMessageLabels),
		delivered:    counter("delivered", "messages", messageUnit, topicMessageLabels),
		bytes:        counter("received", "bytes", byteUnit, topicStreamLabels),
		errors:       counter("session", "errors", "{error}", topicSessionErrorLabels),
		queued:       counter("commit", "queued", messageUnit, topicMessageLabels),
		acknowledged: counter("commit", "acknowledged", messageUnit, topicMessageLabels),
		local:        gauge("local_buffer", "messages", messageUnit, topicStreamLabels),
		credit:       gauge("", "credit_balance_bytes", byteUnit, topicStreamLabels),
	}
}

func topicMetricName(scope, name string) string {
	const prefix = "ydb.topic.reader."
	if scope == "" {
		return prefix + name
	}

	return prefix + scope + "." + name
}

func topic(config Config) internalConfig.TopicTraces {
	reader := config.WithSystem("topic").WithSystem("reader")
	vectors := registerTopicVectors(reader)
	// Snapshot details when installing the hooks: disabled hooks stay nil, and
	// a stream's closing gauge delta uses the same mask as its reservation.
	details := reader.Details()

	return internalConfig.TopicTraces{
		Reader:   topicTrace(details&(trace.TopicReaderMessageEvents|trace.TopicReaderStreamEvents), vectors),
		Listener: topicTrace(details&trace.TopicListenerStreamEvents, vectors),
	}
}

func topicTrace(details trace.Details, vectors topicVectors) *trace.Topic {
	if details == 0 {
		return nil
	}
	t := &trace.Topic{}
	setMessageHooks(t, details, vectors)

	if details&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) != 0 {
		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			if info.MessagesCount > 0 {
				labels := messageLabels(info.ReaderInfo, info.Topic)
				addCounter(vectors.acknowledged(labels), info.MessagesCount)
			}
		}
		t.OnReaderSessionError = func(info trace.TopicReaderSessionErrorInfo) {
			labels := streamLabels(info.ReaderInfo)
			labels["retry_decision"] = info.RetryDecision
			labels["status_code"] = info.StatusCode
			labels["error.type"] = info.ErrorType
			vectors.errors(labels).Inc()
		}
	}
	setCommitQueuedHooks(t, details, vectors)
	setReceivedBytesHooks(t, details, vectors)

	return t
}

func setMessageHooks(t *trace.Topic, details trace.Details, vectors topicVectors) {
	received := func(info trace.TopicReaderMessagesReceivedInfo) {
		addCounter(vectors.received(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
	}
	delivered := func(info trace.TopicReaderMessagesDeliveredInfo) {
		addCounter(vectors.delivered(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
	}
	credit := func(info trace.TopicReaderCreditBalanceChangedInfo) {
		addGauge(vectors.credit(streamLabels(info.ReaderInfo)), float64(info.BytesDelta))
	}
	local := func(info trace.TopicReaderLocalBufferChangedInfo) {
		addGauge(vectors.local(streamLabels(info.ReaderInfo)), float64(info.MessagesDelta))
	}
	if details&trace.TopicReaderMessageEvents != 0 {
		t.OnReaderMessagesReceived = received
		t.OnReaderMessagesDelivered = delivered
		t.OnReaderCreditBalanceChanged = credit
		t.OnReaderLocalBufferChanged = local
	}
	if details&trace.TopicListenerStreamEvents != 0 {
		t.OnListenerMessagesReceived = received
		t.OnListenerMessagesDelivered = delivered
		t.OnListenerCreditBalanceChanged = credit
		t.OnListenerLocalBufferChanged = local
	}
}

// Each reader kind tracks pending commits only when its own queued hook is set.
func setCommitQueuedHooks(t *trace.Topic, details trace.Details, vectors topicVectors) {
	queued := func(info trace.TopicReaderCommitQueuedInfo) {
		addCounter(vectors.queued(messageLabels(info.ReaderInfo, info.Topic)), info.MessagesCount)
	}
	if details&trace.TopicReaderStreamEvents != 0 {
		t.OnReaderCommitQueued = queued
	}
	if details&trace.TopicListenerStreamEvents != 0 {
		t.OnListenerCommitQueued = queued
	}
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

func descriptorBinding[T any](bind topicBinding[T]) topicBinding[T] {
	return func(labels map[string]string) T {
		if labels[consumerLabel] == "" {
			delete(labels, consumerLabel)
		}

		return bind(labels)
	}
}

func legacyTopicLabel(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func legacyTopicLabelNames(names []string) []string {
	legacy := make([]string, len(names))
	for i, name := range names {
		legacy[i] = legacyTopicLabel(name)
	}

	return legacy
}

func legacyTopicBinding[T any](bind topicBinding[T]) topicBinding[T] {
	return func(labels map[string]string) T {
		for name, value := range labels {
			if legacy := legacyTopicLabel(name); legacy != name {
				delete(labels, name)
				labels[legacy] = value
			}
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

func addCounter[T int | int64](counter Counter, delta T) {
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
