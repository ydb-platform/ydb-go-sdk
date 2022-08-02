package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type CreateOption func(request *rawtopic.CreateTopicRequest)

func CreateWithMinActivePartitions(count int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionSettings.PartitionCountLimit = count
	}
}

func CreateWithPartitionCountLimit(count int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionSettings.PartitionCountLimit = count
	}
}

func CreateWithRetentionPeriod(retentionPeriod time.Duration) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.RetentionPeriod = retentionPeriod
	}
}

func CreateWithRetentionStorageMB(retentionStorageMB int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.RetentionStorageMB = retentionStorageMB
	}
}

func CreateWithPartitionWriteSpeedBytesPerSecond(partitionWriteSpeedBytesPerSecond int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionWriteSpeedBytesPerSecond = partitionWriteSpeedBytesPerSecond
	}
}

func CreateWithPartitionWriteBurstBytes(partitionWriteBurstBytes int64) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.PartitionWriteBurstBytes = partitionWriteBurstBytes
	}
}

func CreateWithAttributes(attributes map[string]string) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.Attributes = attributes
	}
}

func CreateWithConsumer(consumers ...topictypes.Consumer) CreateOption {
	return func(request *rawtopic.CreateTopicRequest) {
		request.Consumers = make([]rawtopic.Consumer, len(consumers))
		for i := range consumers {
			consumers[i].ToRaw(&request.Consumers[i])
		}
	}
}
