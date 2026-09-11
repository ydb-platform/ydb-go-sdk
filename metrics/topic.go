package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

const (
	topicReaderReceivedMessagesName    = "ydb.topic.reader.received.messages"
	topicReaderReceivedMessagesUnit    = "{message}"
	topicReaderDeliveredMessagesName   = "ydb.topic.reader.delivered.messages"
	topicReaderDeliveredMessagesUnit   = "{message}"
	topicReaderReceivedBytesName       = "ydb.topic.reader.received.bytes"
	topicReaderReceivedBytesUnit       = "By"
	topicReaderSessionErrorsName       = "ydb.topic.reader.session.errors"
	topicReaderSessionErrorsUnit       = "{error}"
	topicReaderCommitQueuedName        = "ydb.topic.reader.commit.queued"
	topicReaderCommitQueuedUnit        = "{message}"
	topicReaderCommitAcknowledgedName  = "ydb.topic.reader.commit.acknowledged"
	topicReaderCommitAcknowledgedUnit  = "{message}"
	topicReaderLocalBufferMessagesName = "ydb.topic.reader.local_buffer.messages"
	topicReaderLocalBufferMessagesUnit = "{message}"
	topicReaderCreditBalanceBytesName  = "ydb.topic.reader.credit_balance_bytes"
	topicReaderCreditBalanceBytesUnit  = "By"
)

var (
	topicMessageLabels      = []string{"endpoint", "database", "topic", "consumer", "reader.name"}
	topicStreamLabels       = []string{"endpoint", "database", "consumer", "reader.name"}
	topicSessionErrorLabels = []string{
		"endpoint", "database", "consumer", "reader.name",
		"retry_decision", "status_code", "error.type",
	}
)

func topic(config Config) (t trace.Topic) {
	readerConfig := config.
		WithSystem("topic").
		WithSystem("reader")

	setupTopicReaderReceivedMessages(&t, readerConfig)
	setupTopicReaderDeliveredMessages(&t, readerConfig)
	setupTopicReaderReceivedBytes(&t, readerConfig)
	setupTopicReaderSessionErrors(&t, readerConfig)
	setupTopicReaderCommitQueued(&t, readerConfig)
	setupTopicReaderCommitAcknowledged(&t, readerConfig)
	setupTopicReaderLocalBufferMessages(&t, readerConfig)
	setupTopicReaderCreditBalanceBytes(&t, readerConfig)
	setupTopicReaderObservableMetrics(&t, readerConfig)

	return t
}

func setupTopicReaderReceivedMessages(t *trace.Topic, config Config) {
	messages := topicCounter(
		config,
		"received",
		"messages",
		topicReaderReceivedMessagesName,
		topicReaderReceivedMessagesUnit,
		topicMessageLabels,
	)
	if config.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderMessagesReceived = func(info trace.TopicReaderMessagesReceivedInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderMessageEvents) {
			return
		}
		addCounter(
			messages.With(messageLabels(info.Endpoint, info.Database, info.Topic, info.Consumer, info.ReaderName)),
			info.MessagesCount,
		)
	}
}

func setupTopicReaderDeliveredMessages(t *trace.Topic, config Config) {
	deliveredMessages := topicCounter(
		config,
		"delivered",
		"messages",
		topicReaderDeliveredMessagesName,
		topicReaderDeliveredMessagesUnit,
		topicMessageLabels,
	)
	if config.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderMessagesDelivered = func(info trace.TopicReaderMessagesDeliveredInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderMessageEvents) {
			return
		}
		addCounter(
			deliveredMessages.With(messageLabels(info.Endpoint, info.Database, info.Topic, info.Consumer, info.ReaderName)),
			info.MessagesCount,
		)
	}
}

func setupTopicReaderReceivedBytes(t *trace.Topic, config Config) {
	receivedBytes := topicCounter(
		config,
		"received",
		"bytes",
		topicReaderReceivedBytesName,
		topicReaderReceivedBytesUnit,
		topicStreamLabels,
	)
	if config.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderReceivedBytes = func(info trace.TopicReaderReceivedBytesInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderMessageEvents) {
			return
		}
		if info.Bytes <= 0 {
			return
		}
		addCounterWithAdd(
			receivedBytes.With(streamLabels(info.Endpoint, info.Database, info.Consumer, info.ReaderName)),
			info.Bytes,
		)
	}
}

func setupTopicReaderSessionErrors(t *trace.Topic, config Config) {
	sessionErrors := topicCounter(
		config,
		"session",
		"errors",
		topicReaderSessionErrorsName,
		topicReaderSessionErrorsUnit,
		topicSessionErrorLabels,
	)
	if config.Details()&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderSessionError = func(info trace.TopicReaderSessionErrorInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderStreamEvents) {
			return
		}
		labels := streamLabels(info.Endpoint, info.Database, info.Consumer, info.ReaderName)
		labels["retry_decision"] = info.RetryDecision
		labels["status_code"] = info.StatusCode
		labels["error.type"] = info.ErrorType
		addCounter(sessionErrors.With(labels), 1)
	}
}

func setupTopicReaderCommitQueued(t *trace.Topic, config Config) {
	commitQueued := topicCounter(
		config,
		"commit",
		"queued",
		topicReaderCommitQueuedName,
		topicReaderCommitQueuedUnit,
		topicMessageLabels,
	)
	if config.Details()&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderCommitQueued = func(info trace.TopicReaderCommitQueuedInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderStreamEvents) {
			return
		}
		addCounterWithAdd(
			commitQueued.With(messageLabels(info.Endpoint, info.Database, info.Topic, info.Consumer, info.ReaderName)),
			info.MessagesCount,
		)
	}
}

func setupTopicReaderCommitAcknowledged(t *trace.Topic, config Config) {
	commitAcknowledged := topicCounter(
		config,
		"commit",
		"acknowledged",
		topicReaderCommitAcknowledgedName,
		topicReaderCommitAcknowledgedUnit,
		topicMessageLabels,
	)
	if config.Details()&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderCommitAcknowledged = func(info trace.TopicReaderCommitAcknowledgedInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderStreamEvents) {
			return
		}
		addCounterWithAdd(
			commitAcknowledged.With(messageLabels(info.Endpoint, info.Database, info.Topic, info.Consumer, info.ReaderName)),
			info.MessagesCount,
		)
	}
}

func setupTopicReaderLocalBufferMessages(t *trace.Topic, config Config) {
	localBufferMessages := topicGauge(
		config,
		"local_buffer",
		"messages",
		topicReaderLocalBufferMessagesName,
		topicReaderLocalBufferMessagesUnit,
		topicMessageLabels,
	)
	if config.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderLocalBufferChanged = func(info trace.TopicReaderLocalBufferChangedInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderMessageEvents) {
			return
		}
		addGauge(
			localBufferMessages.With(messageLabels(info.Endpoint, info.Database, info.Topic, info.Consumer, info.ReaderName)),
			float64(info.MessagesDelta),
		)
	}
}

func setupTopicReaderCreditBalanceBytes(t *trace.Topic, config Config) {
	creditBalanceBytes := topicGauge(
		config,
		"",
		"credit_balance_bytes",
		topicReaderCreditBalanceBytesName,
		topicReaderCreditBalanceBytesUnit,
		topicStreamLabels,
	)
	if config.Details()&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) == 0 {
		return
	}
	t.OnReaderCreditBalanceChanged = func(info trace.TopicReaderCreditBalanceChangedInfo) {
		if !topicMetricEnabled(config.Details(), info.Listener, trace.TopicReaderMessageEvents) {
			return
		}
		addGauge(
			creditBalanceBytes.With(streamLabels(info.Endpoint, info.Database, info.Consumer, info.ReaderName)),
			float64(info.BytesDelta),
		)
	}
}

func topicCounter(readerConfig Config, legacySystem, legacyName, name, unit string, labels []string) CounterVec {
	if registry, ok := readerConfig.(RegistryWithDescriptors); ok {
		return registry.CounterVecWithDescriptor(name, unit, labels...)
	}

	return readerConfig.WithSystem(legacySystem).CounterVec(legacyName, labels...)
}

func topicGauge(readerConfig Config, legacySystem, legacyName, name, unit string, labels []string) GaugeVec {
	if registry, ok := readerConfig.(RegistryWithGaugeDescriptors); ok {
		return registry.GaugeVecWithDescriptor(name, unit, labels...)
	}

	if legacySystem == "" {
		return readerConfig.GaugeVec(legacyName, labels...)
	}

	return readerConfig.WithSystem(legacySystem).GaugeVec(legacyName, labels...)
}

func messageLabels(endpoint, database, topic, consumer string, readerName *string) map[string]string {
	labels := streamLabels(endpoint, database, consumer, readerName)
	labels["topic"] = topic

	return labels
}

func streamLabels(endpoint, database, consumer string, readerName *string) map[string]string {
	readerNameValue := ""
	if readerName != nil {
		readerNameValue = *readerName
	}

	return map[string]string{
		"endpoint":    endpoint,
		"database":    database,
		"consumer":    consumer,
		"reader.name": readerNameValue,
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
	for range delta {
		counter.Inc()
	}
}

func addCounterWithAdd(counter Counter, delta int) {
	if delta <= 0 {
		return
	}
	adder, ok := counter.(interface{ Add(delta int64) })
	if !ok {
		return
	}

	adder.Add(int64(delta))
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
