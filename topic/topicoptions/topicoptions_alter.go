package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// AlterOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type AlterOption func(req *rawtopic.AlterTopicRequest)

// AlterWithMinActivePartitions
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithMinActivePartitions(minActivePartitions int64) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.AlterPartitionSettings.SetMinActivePartitions.HasValue = true
		req.AlterPartitionSettings.SetMinActivePartitions.Value = minActivePartitions
	}
}

// AlterWithPartitionCountLimit
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionCountLimit(partitionCountLimit int64) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.AlterPartitionSettings.SetPartitionCountLimit.HasValue = true
		req.AlterPartitionSettings.SetPartitionCountLimit.Value = partitionCountLimit
	}
}

// AlterWithRetentionPeriod
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithRetentionPeriod(retentionPeriod time.Duration) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.SetRetentionPeriod.HasValue = true
		req.SetRetentionPeriod.Value = retentionPeriod
	}
}

// AlterWithRetentionStorageMB
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithRetentionStorageMB(retentionStorageMB int64) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.SetRetentionStorageMB.HasValue = true
		req.SetRetentionStorageMB.Value = retentionStorageMB
	}
}

// AlterWithSupportedCodecs
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithSupportedCodecs(codecs ...topictypes.Codec) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.SetSupportedCodecs = true
		req.SetSupportedCodecsValue = make(rawtopiccommon.SupportedCodecs, len(codecs))
		for i, codec := range codecs {
			req.SetSupportedCodecsValue[i] = rawtopiccommon.Codec(codec)
		}
	}
}

// AlterWithPartitionWriteSpeedBytesPerSecond
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionWriteSpeedBytesPerSecond(bytesPerSecond int64) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.SetPartitionWriteSpeedBytesPerSecond.HasValue = true
		req.SetPartitionWriteSpeedBytesPerSecond.Value = bytesPerSecond
	}
}

// AlterWithPartitionWriteBurstBytes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionWriteBurstBytes(burstBytes int64) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.SetPartitionWriteBurstBytes.HasValue = true
		req.SetPartitionWriteBurstBytes.Value = burstBytes
	}
}

// AlterWithAttributes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithAttributes(attributes map[string]string) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.AlterAttributes = attributes
	}
}

// AlterWithAddConsumers
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithAddConsumers(consumers ...topictypes.Consumer) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.AddConsumers = make([]rawtopic.Consumer, len(consumers))
		for i := range consumers {
			consumers[i].ToRaw(&req.AddConsumers[i])
		}
	}
}

// AlterWithDropConsumers
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithDropConsumers(consumersName ...string) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		req.DropConsumers = consumersName
	}
}

// AlterConsumerWithImportant
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithImportant(name string, important bool) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		var index int
		req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, name)
		req.AlterConsumers[index].SetImportant.HasValue = true
		req.AlterConsumers[index].SetImportant.Value = important
	}
}

// AlterConsumerWithReadFrom
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithReadFrom(name string, readFrom time.Time) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		var index int
		req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, name)
		req.AlterConsumers[index].SetReadFrom.HasValue = true
		req.AlterConsumers[index].SetReadFrom.Value = readFrom
	}
}

// AlterConsumerWithSupportedCodecs
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithSupportedCodecs(name string, codecs []topictypes.Codec) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		var index int
		req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, name)

		consumer := &req.AlterConsumers[index]
		consumer.SetSupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(codecs))
		for i, codec := range codecs {
			consumer.SetSupportedCodecs[i] = rawtopiccommon.Codec(codec)
		}
	}
}

// AlterConsumerWithAttributes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithAttributes(name string, attributes map[string]string) AlterOption {
	return func(req *rawtopic.AlterTopicRequest) {
		var index int
		req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, name)
		req.AlterConsumers[index].AlterAttributes = attributes
	}
}

func ensureAlterConsumer(
	consumers []rawtopic.AlterConsumer,
	name string,
) (
	newConsumers []rawtopic.AlterConsumer,
	index int,
) {
	for i := range consumers {
		if consumers[i].Name == name {
			return consumers, i
		}
	}
	consumers = append(consumers, rawtopic.AlterConsumer{Name: name})
	return consumers, len(consumers) - 1
}
