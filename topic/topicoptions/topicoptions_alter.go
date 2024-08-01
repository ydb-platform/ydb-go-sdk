package topicoptions

import (
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// AlterOption type of options for change topic settings
type AlterOption interface {
	internal.Interface
	ApplyAlterOption(req *rawtopic.AlterTopicRequest)
}

// AlterWithMeteringMode change metering mode for topic (need for serverless installations)
func AlterWithMeteringMode(m topictypes.MeteringMode) AlterOption {
	return withMeteringMode{val: m}
}

// AlterWithMinActivePartitions change min active partitions of the topic
func AlterWithMinActivePartitions(minActivePartitions int64) AlterOption {
	return withMinActivePartitions{val: minActivePartitions}
}

// AlterWithPartitionCountLimit change partition count limit of the topic
func AlterWithPartitionCountLimit(partitionCountLimit int64) AlterOption {
	return withPartitionCountLimit{val: partitionCountLimit}
}

// AlterWithRetentionPeriod change retention period of topic
func AlterWithRetentionPeriod(retentionPeriod time.Duration) AlterOption {
	return withRetentionPeriod{val: retentionPeriod}
}

// AlterWithRetentionStorageMB change retention storage size in MB.
func AlterWithRetentionStorageMB(retentionStorageMB int64) AlterOption {
	return withRetentionStorageMB{val: retentionStorageMB}
}

// AlterWithSupportedCodecs change set of codec, allowed for the topic
func AlterWithSupportedCodecs(codecs ...topictypes.Codec) AlterOption {
	sort.Slice(codecs, func(i, j int) bool {
		return codecs[i] < codecs[j]
	})

	return withSupportedCodecs{val: codecs}
}

// AlterWithPartitionWriteSpeedBytesPerSecond change limit of write speed for partitions of the topic
func AlterWithPartitionWriteSpeedBytesPerSecond(bytesPerSecond int64) AlterOption {
	return withPartitionWriteSpeedBytesPerSecond{val: bytesPerSecond}
}

// AlterWithPartitionWriteBurstBytes change burst size for write to partition of topic
func AlterWithPartitionWriteBurstBytes(burstBytes int64) AlterOption {
	return withPartitionWriteBurstBytes{val: burstBytes}
}

// AlterWithAttributes change attributes map of topic
func AlterWithAttributes(attributes map[string]string) AlterOption {
	return withAttributes{val: attributes}
}

// AlterWithAddConsumers add consumer to the topic
func AlterWithAddConsumers(consumers ...topictypes.Consumer) AlterOption {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})

	return withAddConsumers{val: consumers}
}

// AlterWithDropConsumers drop consumer from the topic
func AlterWithDropConsumers(consumersName ...string) AlterOption {
	sort.Strings(consumersName)

	return withDropConsumers{consumersName: consumersName}
}

// AlterConsumerWithImportant set/remove important flag for the consumer of topic
func AlterConsumerWithImportant(name string, important bool) AlterOption {
	return withConsumerWithImportant{
		name:      name,
		important: important,
	}
}

// AlterConsumerWithReadFrom change min time of messages, received for the topic
func AlterConsumerWithReadFrom(name string, readFrom time.Time) AlterOption {
	return withConsumerWithReadFrom{
		name:     name,
		readFrom: readFrom,
	}
}

// AlterConsumerWithSupportedCodecs change codecs, supported by the consumer
func AlterConsumerWithSupportedCodecs(name string, codecs []topictypes.Codec) AlterOption {
	sort.Slice(codecs, func(i, j int) bool {
		return codecs[i] < codecs[j]
	})

	return withConsumerWithSupportedCodecs{
		name:   name,
		codecs: codecs,
	}
}

// AlterConsumerWithAttributes change attributes of the consumer
func AlterConsumerWithAttributes(name string, attributes map[string]string) AlterOption {
	return withConsumerWithAttributes{
		name:       name,
		attributes: attributes,
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
