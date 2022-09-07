package topicwriterinternal

type PublicWriterOption func(cfg *writerImplConfig)

func WithAutoSetSeqNo(val bool) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.autoSetSeqNo = val
	}
}

func WithConnectFunc(connect ConnectFunc) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.connect = connect
	}
}

func WithPartitioning(partitioning PublicPartitioning) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.defaultPartitioning = partitioning.ToRaw()
	}
}

func WithProducerID(producerID string) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.producerID = producerID
	}
}

func WithSessionMeta(meta map[string]string) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		if len(meta) == 0 {
			cfg.writerMeta = nil
		} else {
			cfg.writerMeta = make(map[string]string, len(meta))
			for k, v := range meta {
				cfg.writerMeta[k] = v
			}
		}
	}
}

func WithWaitAckOnWrite(val bool) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.waitServerAck = val
	}
}

func WithTopic(topic string) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.topic = topic
	}
}
