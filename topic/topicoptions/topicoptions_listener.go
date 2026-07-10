package topicoptions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// ListenerOption set settings for topic listener struct
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type ListenerOption func(cfg *topiclistenerinternal.StreamListenerConfig)

// WithListenerBufferSizeBytes sets size of the internal read-ahead buffer in bytes.
// Flow control matches topic reader: one shared BufferSize limits in-flight data
// across all partitions. Default value is the same as for topic reader: 1 MiB.
//
// Size must be positive; a non-positive value is rejected during listener creation.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithListenerBufferSizeBytes(size int) ListenerOption {
	return func(cfg *topiclistenerinternal.StreamListenerConfig) {
		cfg.BufferSize = size
	}
}

// WithListenerAddDecoder add decoder for a codec.
// It allows to set decoders fabric for custom codec and replace internal decoders.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithListenerAddDecoder(codec topictypes.Codec, decoderCreate CreateDecoderFunc) ListenerOption {
	return func(cfg *topiclistenerinternal.StreamListenerConfig) {
		cfg.Decoders.AddDecoder(rawtopiccommon.Codec(codec), decoderCreate)
	}
}
