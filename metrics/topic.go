package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

const (
	topicReaderReceivedMessagesName = "ydb.topic.reader.received.messages"
	topicReaderReceivedMessagesUnit = "{message}"
)

func topic(config Config) trace.Topic {
	return topicWithRoot(config, config)
}

func topicWithRoot(config, rootConfig Config) (t trace.Topic) {
	readerConfig := config.
		WithSystem("topic").
		WithSystem("reader")
	messages := topicReceivedMessagesCounter(rootConfig, readerConfig)

	t.OnReaderMessagesReceived = func(info trace.TopicReaderMessagesReceivedInfo) {
		if readerConfig.Details()&trace.TopicReaderMessageEvents == 0 || info.MessagesCount <= 0 {
			return
		}
		addCounter(messages.With(map[string]string{
			"endpoint": info.Endpoint,
			"database": info.Database,
			"topic":    info.Topic,
			"consumer": info.Consumer,
		}), info.MessagesCount)
	}
	// Listener batches represent the same accepted message stream, but are
	// emitted through a separate callback so reader-only detail selection does
	// not accidentally enable listener data (or vice versa).
	t.OnListenerMessagesReceived = func(info trace.TopicListenerMessagesReceivedInfo) {
		if readerConfig.Details()&trace.TopicListenerEvents == 0 || info.MessagesCount <= 0 {
			return
		}
		addCounter(messages.With(map[string]string{
			"endpoint": info.Endpoint,
			"database": info.Database,
			"topic":    info.Topic,
			"consumer": info.Consumer,
		}), info.MessagesCount)
	}

	return t
}

func topicReceivedMessagesCounter(rootConfig, readerConfig Config) CounterVec {
	const (
		labelEndpoint = "endpoint"
		labelDatabase = "database"
		labelTopic    = "topic"
		labelConsumer = "consumer"
	)
	labels := []string{labelEndpoint, labelDatabase, labelTopic, labelConsumer}

	if registry, ok := readerConfig.(RegistryWithDescriptors); ok {
		return registry.CounterVecWithDescriptor(topicReaderReceivedMessagesName, topicReaderReceivedMessagesUnit, labels...)
	}
	// A custom Config may return a legacy wrapper from WithSystem even when its
	// root also supports the optional capability. Use the root as a last resort
	// so scoping cannot silently discard descriptor support.
	if registry, ok := rootConfig.(RegistryWithDescriptors); ok {
		return registry.CounterVecWithDescriptor(topicReaderReceivedMessagesName, topicReaderReceivedMessagesUnit, labels...)
	}

	return readerConfig.
		WithSystem("received").
		CounterVec("messages", labels...)
}

func addCounter(counter Counter, delta int) {
	if delta <= 0 {
		return
	}
	if adder, ok := counter.(CounterAdder); ok {
		adder.Add(int64(delta))

		return
	}
	for range delta {
		counter.Inc()
	}
}
