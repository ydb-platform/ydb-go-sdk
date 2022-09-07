package topicwriterinternal

import "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"

type PublicWriterOption func(cfg *writerImplConfig)

func WithAutoSetSeqNo(val bool) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.autoSetSeqNo = val
	}
}

func WithAutoCodec() PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.forceCodec = rawtopiccommon.CodecUNSPECIFIED
	}
}

func WithCodec(codec rawtopiccommon.Codec) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.forceCodec = codec
	}
}

func WithConnectFunc(connect ConnectFunc) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.connect = connect
	}
}

func WithAutosetCreatedTime(enable bool) PublicWriterOption {
	return func(cfg *writerImplConfig) {
		cfg.fillEmptyCreatedTime = enable
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
