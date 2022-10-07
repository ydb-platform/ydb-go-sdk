package topicoptions

import (
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// AlterOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type AlterOption interface {
	ApplyAlterOption(req *rawtopic.AlterTopicRequest)
}

// AlterWithMeteringMode
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithMeteringMode(m topictypes.MeteringMode) AlterOption {
	return withMeteringMode(m)
}

// AlterWithMinActivePartitions
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithMinActivePartitions(minActivePartitions int64) AlterOption {
	return withMinActivePartitions(minActivePartitions)
}

// AlterWithPartitionCountLimit
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionCountLimit(partitionCountLimit int64) AlterOption {
	return withPartitionCountLimit(partitionCountLimit)
}

// AlterWithRetentionPeriod
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithRetentionPeriod(retentionPeriod time.Duration) AlterOption {
	return withRetentionPeriod(retentionPeriod)
}

// AlterWithRetentionStorageMB
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithRetentionStorageMB(retentionStorageMB int64) AlterOption {
	return withRetentionStorageMB(retentionStorageMB)
}

// AlterWithSupportedCodecs
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithSupportedCodecs(codecs ...topictypes.Codec) AlterOption {
	sort.Slice(codecs, func(i, j int) bool {
		return codecs[i] < codecs[j]
	})
	return withSupportedCodecs(codecs)
}

// AlterWithPartitionWriteSpeedBytesPerSecond
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionWriteSpeedBytesPerSecond(bytesPerSecond int64) AlterOption {
	return withPartitionWriteSpeedBytesPerSecond(bytesPerSecond)
}

// AlterWithPartitionWriteBurstBytes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithPartitionWriteBurstBytes(burstBytes int64) AlterOption {
	return withPartitionWriteBurstBytes(burstBytes)
}

// AlterWithAttributes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithAttributes(attributes map[string]string) AlterOption {
	return withAttributes(attributes)
}

// AlterWithAddConsumers
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithAddConsumers(consumers ...topictypes.Consumer) AlterOption {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})
	return withAddConsumers(consumers)
}

// AlterWithDropConsumers
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterWithDropConsumers(consumersName ...string) AlterOption {
	sort.Strings(consumersName)
	return withDropConsumers(consumersName)
}

// AlterConsumerWithImportant
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithImportant(name string, important bool) AlterOption {
	return withConsumerWithImportant{
		name:      name,
		important: important,
	}
}

// AlterConsumerWithReadFrom
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithReadFrom(name string, readFrom time.Time) AlterOption {
	return withConsumerWithReadFrom{
		name:     name,
		readFrom: readFrom,
	}
}

// AlterConsumerWithSupportedCodecs
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func AlterConsumerWithSupportedCodecs(name string, codecs []topictypes.Codec) AlterOption {
	sort.Slice(codecs, func(i, j int) bool {
		return codecs[i] < codecs[j]
	})
	return withConsumerWithSupportedCodecs{
		name:   name,
		codecs: codecs,
	}
}

// AlterConsumerWithAttributes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
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
