package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

// CreateOption
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type CreateOption func(request *rawtopic.CreateTopicRequest)

// CreateWithMinActivePartitions
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithMinActivePartitions(count int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionSettings.MinActivePartitions = count
	}
}

// CreateWithPartitionCountLimit
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionCountLimit(count int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionSettings.PartitionCountLimit = count
	}
}

// CreateWithRetentionPeriod
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithRetentionPeriod(retentionPeriod time.Duration) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.RetentionPeriod = retentionPeriod
	}
}

// CreateWithRetentionStorageMB
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithRetentionStorageMB(retentionStorageMB int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.RetentionStorageMB = retentionStorageMB
	}
}

// CreateWithPartitionWriteSpeedBytesPerSecond
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionWriteSpeedBytesPerSecond(partitionWriteSpeedBytesPerSecond int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionWriteSpeedBytesPerSecond = partitionWriteSpeedBytesPerSecond
	}
}

// CreateWithPartitionWriteBurstBytes
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithPartitionWriteBurstBytes(partitionWriteBurstBytes int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionWriteBurstBytes = partitionWriteBurstBytes
	}
}

// CreateWithAttributes
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithAttributes(attributes map[string]string) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.Attributes = attributes
	}
}

// CreateWithConsumer
//
// Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func CreateWithConsumer(consumers ...topictypes.Consumer) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.Consumers = make([]rawtopic.Consumer, len(consumers))
		for i := range consumers {
			consumers[i].ToRaw(&request.Consumers[i])
		}
	}
}
