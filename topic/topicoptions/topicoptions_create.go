package topicoptions

import (
	"sort"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// CreateOption
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CreateOption interface {
	Create(request *rawtopic.CreateTopicRequest)
}

// CreateWithMeteringMode
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithMeteringMode(mode topictypes.MeteringMode) CreateOption {
	return withMeteringMode(mode)
}

// CreateWithMinActivePartitions
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithMinActivePartitions(count int64) CreateOption {
	return withMinActivePartitions(count)
}

// CreateWithPartitionCountLimit
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionCountLimit(count int64) CreateOption {
	return withPartitionCountLimit(count)
}

// CreateWithRetentionPeriod
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithRetentionPeriod(retentionPeriod time.Duration) CreateOption {
	return withRetentionPeriod(retentionPeriod)
}

// CreateWithRetentionStorageMB
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithRetentionStorageMB(retentionStorageMB int64) CreateOption {
	return withRetentionStorageMB(retentionStorageMB)
}

// CreateWithSupportedCodecs
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithSupportedCodecs(codecs ...topictypes.Codec) CreateOption {
	sort.Slice(codecs, func(i, j int) bool {
		return codecs[i] < codecs[j]
	})
	return withSupportedCodecs(codecs)
}

// CreateWithPartitionWriteSpeedBytesPerSecond
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionWriteSpeedBytesPerSecond(partitionWriteSpeedBytesPerSecond int64) CreateOption {
	return withPartitionWriteSpeedBytesPerSecond(partitionWriteSpeedBytesPerSecond)
}

// CreateWithPartitionWriteBurstBytes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionWriteBurstBytes(partitionWriteBurstBytes int64) CreateOption {
	return withPartitionWriteBurstBytes(partitionWriteBurstBytes)
}

// CreateWithAttributes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithAttributes(attributes map[string]string) CreateOption {
	return withAttributes(attributes)
}

// CreateWithConsumer
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithConsumer(consumers ...topictypes.Consumer) CreateOption {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].Name < consumers[j].Name
	})
	return withAddConsumers(consumers)
}
