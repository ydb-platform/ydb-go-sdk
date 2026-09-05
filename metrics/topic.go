package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

const (
	topicReaderReceivedMessagesName = "ydb.topic.reader.received.messages"
	topicReaderReceivedMessagesUnit = "{message}"
)

func topic(config Config) (t trace.Topic) {
	readerConfig := config.
		WithSystem("topic").
		WithSystem("reader")
	messages := topicReceivedMessagesCounter(readerConfig)

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

	return t
}

func topicReceivedMessagesCounter(readerConfig Config) CounterVec {
	labels := []string{"endpoint", "database", "topic", "consumer"}

	if registry, ok := readerConfig.(RegistryWithDescriptors); ok {
		return registry.CounterVecWithDescriptor(topicReaderReceivedMessagesName, topicReaderReceivedMessagesUnit, labels...)
	}

	return readerConfig.
		WithSystem("received").
		CounterVec("messages", labels...)
}

func addCounter(counter Counter, delta int) {
	if adder, ok := counter.(CounterAdder); ok {
		adder.Add(int64(delta))

		return
	}
	for range delta {
		counter.Inc()
	}
}
