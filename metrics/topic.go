package metrics

import "github.com/ydb-platform/ydb-go-sdk/v3/trace"

func topic(config Config) (t trace.Topic) {
	config = config.
		WithSystem("topic").
		WithSystem("reader").
		WithSystem("received")
	messages := config.CounterVec("messages", "endpoint", "database", "topic", "consumer")
	t.OnReaderMessagesReceived = func(info trace.TopicReaderMessagesReceivedInfo) {
		if config.Details()&trace.TopicReaderMessageEvents == 0 || info.MessagesCount <= 0 {
			return
		}
		counter := messages.With(map[string]string{
			"endpoint": info.Endpoint,
			"database": info.Database,
			"topic":    info.Topic,
			"consumer": info.Consumer,
		})
		for range info.MessagesCount {
			counter.Inc()
		}
	}

	return
}
