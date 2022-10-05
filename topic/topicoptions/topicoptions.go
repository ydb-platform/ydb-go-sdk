package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type withMeteringMode topictypes.MeteringMode

func (mode withMeteringMode) Create(request *rawtopic.CreateTopicRequest) {
	(*topictypes.MeteringMode)(&mode).ToRaw(&request.MeteringMode)
}

func (mode withMeteringMode) Alter(req *rawtopic.AlterTopicRequest) {
	(*topictypes.MeteringMode)(&mode).ToRaw(&req.SetMeteringMode)
}

type withMinActivePartitions int64

func (minActivePartitions withMinActivePartitions) Create(request *rawtopic.CreateTopicRequest) {
	request.PartitionSettings.MinActivePartitions = int64(minActivePartitions)
}

func (minActivePartitions withMinActivePartitions) Alter(req *rawtopic.AlterTopicRequest) {
	req.AlterPartitionSettings.SetMinActivePartitions.HasValue = true
	req.AlterPartitionSettings.SetMinActivePartitions.Value = int64(minActivePartitions)
}

type withPartitionCountLimit int64

func (partitionCountLimit withPartitionCountLimit) Create(request *rawtopic.CreateTopicRequest) {
	request.PartitionSettings.PartitionCountLimit = int64(partitionCountLimit)
}

func (partitionCountLimit withPartitionCountLimit) Alter(req *rawtopic.AlterTopicRequest) {
	req.AlterPartitionSettings.SetPartitionCountLimit.HasValue = true
	req.AlterPartitionSettings.SetPartitionCountLimit.Value = int64(partitionCountLimit)
}

type withRetentionPeriod time.Duration

func (retentionPeriod withRetentionPeriod) Create(request *rawtopic.CreateTopicRequest) {
	request.RetentionPeriod = time.Duration(retentionPeriod)
}

func (retentionPeriod withRetentionPeriod) Alter(req *rawtopic.AlterTopicRequest) {
	req.SetRetentionPeriod.HasValue = true
	req.SetRetentionPeriod.Value = time.Duration(retentionPeriod)
}

type withRetentionStorageMB int64

func (retentionStorageMB withRetentionStorageMB) Create(request *rawtopic.CreateTopicRequest) {
	request.RetentionStorageMB = int64(retentionStorageMB)
}

func (retentionStorageMB withRetentionStorageMB) Alter(req *rawtopic.AlterTopicRequest) {
	req.SetRetentionStorageMB.HasValue = true
	req.SetRetentionStorageMB.Value = int64(retentionStorageMB)
}

type withSupportedCodecs []topictypes.Codec

func (codecs withSupportedCodecs) Create(request *rawtopic.CreateTopicRequest) {
	request.SupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(codecs))
	for i, c := range codecs {
		c.ToRaw(&request.SupportedCodecs[i])
	}
}

func (codecs withSupportedCodecs) Alter(req *rawtopic.AlterTopicRequest) {
	req.SetSupportedCodecs = true
	req.SetSupportedCodecsValue = make(rawtopiccommon.SupportedCodecs, len(codecs))
	for i, codec := range codecs {
		req.SetSupportedCodecsValue[i] = rawtopiccommon.Codec(codec)
	}
}

type withPartitionWriteSpeedBytesPerSecond int64

func (bytesPerSecond withPartitionWriteSpeedBytesPerSecond) Create(request *rawtopic.CreateTopicRequest) {
	request.PartitionWriteSpeedBytesPerSecond = int64(bytesPerSecond)
}

func (bytesPerSecond withPartitionWriteSpeedBytesPerSecond) Alter(req *rawtopic.AlterTopicRequest) {
	req.SetPartitionWriteSpeedBytesPerSecond.HasValue = true
	req.SetPartitionWriteSpeedBytesPerSecond.Value = int64(bytesPerSecond)
}

type withPartitionWriteBurstBytes int64

func (burstBytes withPartitionWriteBurstBytes) Create(request *rawtopic.CreateTopicRequest) {
	request.PartitionWriteBurstBytes = int64(burstBytes)
}

func (burstBytes withPartitionWriteBurstBytes) Alter(req *rawtopic.AlterTopicRequest) {
	req.SetPartitionWriteBurstBytes.HasValue = true
	req.SetPartitionWriteBurstBytes.Value = int64(burstBytes)
}

type withAttributes map[string]string

func (attributes withAttributes) Create(request *rawtopic.CreateTopicRequest) {
	request.Attributes = attributes
}

func (attributes withAttributes) Alter(req *rawtopic.AlterTopicRequest) {
	req.AlterAttributes = attributes
}

type withAddConsumers []topictypes.Consumer

func (consumers withAddConsumers) Create(request *rawtopic.CreateTopicRequest) {
	request.Consumers = make([]rawtopic.Consumer, len(consumers))
	for i := range consumers {
		consumers[i].ToRaw(&request.Consumers[i])
	}
}

func (consumers withAddConsumers) Alter(req *rawtopic.AlterTopicRequest) {
	req.AddConsumers = make([]rawtopic.Consumer, len(consumers))
	for i := range consumers {
		consumers[i].ToRaw(&req.AddConsumers[i])
	}
}

type withDropConsumers []string

func (consumers withDropConsumers) Alter(req *rawtopic.AlterTopicRequest) {
	req.DropConsumers = consumers
}

type withConsumerWithImportant struct {
	name      string
	important bool
}

func (consumerImportant withConsumerWithImportant) Alter(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerImportant.name)
	req.AlterConsumers[index].SetImportant.HasValue = true
	req.AlterConsumers[index].SetImportant.Value = consumerImportant.important
}

type withConsumerWithReadFrom struct {
	name     string
	readFrom time.Time
}

func (consumerReadFrom withConsumerWithReadFrom) Alter(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerReadFrom.name)
	req.AlterConsumers[index].SetReadFrom.HasValue = true
	req.AlterConsumers[index].SetReadFrom.Value = consumerReadFrom.readFrom
}

type withConsumerWithSupportedCodecs struct {
	name   string
	codecs []topictypes.Codec
}

func (consumerCodecs withConsumerWithSupportedCodecs) Alter(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerCodecs.name)

	consumer := &req.AlterConsumers[index]
	consumer.SetSupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(consumerCodecs.codecs))
	for i, codec := range consumerCodecs.codecs {
		consumer.SetSupportedCodecs[i] = rawtopiccommon.Codec(codec)
	}
}

type withConsumerWithAttributes struct {
	name       string
	attributes map[string]string
}

func (consumerAttributes withConsumerWithAttributes) Alter(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerAttributes.name)
	req.AlterConsumers[index].AlterAttributes = consumerAttributes.attributes
}
