package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type (
	CheckErrorRetryFunction = topic.PublicCheckErrorRetryFunction
	CheckErrorRetryArgs     = topic.PublicCheckErrorRetryArgs
	CheckErrorRetryResult   = topic.PublicCheckRetryResult
)

var (
	CheckErrorRetryDecisionDefault = topic.PublicRetryDecisionDefault // Apply default behavior for the error
	CheckErrorRetryDecisionRetry   = topic.PublicRetryDecisionRetry   // Do once more retry
	CheckErrorRetryDecisionStop    = topic.PublicRetryDecisionStop    // Do not retry
)

type withMeteringMode struct {
	internal.InterfaceImplementation
	val topictypes.MeteringMode
}

func (opt withMeteringMode) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	(&opt.val).ToRaw(&request.MeteringMode)
}

func (opt withMeteringMode) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	(&opt.val).ToRaw(&req.SetMeteringMode)
}

type withMinActivePartitions struct {
	internal.InterfaceImplementation
	val int64
}

func (opt withMinActivePartitions) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.PartitionSettings.MinActivePartitions = opt.val
}

func (opt withMinActivePartitions) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.AlterPartitionSettings.SetMinActivePartitions.HasValue = true
	req.AlterPartitionSettings.SetMinActivePartitions.Value = opt.val
}

type withPartitionCountLimit struct {
	internal.InterfaceImplementation
	val int64
}

func (opt withPartitionCountLimit) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.PartitionSettings.PartitionCountLimit = opt.val
}

func (opt withPartitionCountLimit) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.AlterPartitionSettings.SetPartitionCountLimit.HasValue = true
	req.AlterPartitionSettings.SetPartitionCountLimit.Value = opt.val
}

type withRetentionPeriod struct {
	internal.InterfaceImplementation
	val time.Duration
}

func (opt withRetentionPeriod) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.RetentionPeriod = opt.val
}

func (opt withRetentionPeriod) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.SetRetentionPeriod.HasValue = true
	req.SetRetentionPeriod.Value = opt.val
}

type withRetentionStorageMB struct {
	internal.InterfaceImplementation
	val int64
}

func (opt withRetentionStorageMB) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.RetentionStorageMB = opt.val
}

func (opt withRetentionStorageMB) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.SetRetentionStorageMB.HasValue = true
	req.SetRetentionStorageMB.Value = opt.val
}

type withSupportedCodecs struct {
	internal.InterfaceImplementation
	val []topictypes.Codec
}

func (opt withSupportedCodecs) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.SupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(opt.val))
	for i, c := range opt.val {
		c.ToRaw(&request.SupportedCodecs[i])
	}
}

func (opt withSupportedCodecs) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.SetSupportedCodecs = true
	req.SetSupportedCodecsValue = make(rawtopiccommon.SupportedCodecs, len(opt.val))
	for i, codec := range opt.val {
		req.SetSupportedCodecsValue[i] = rawtopiccommon.Codec(codec)
	}
}

type withPartitionWriteSpeedBytesPerSecond struct {
	internal.InterfaceImplementation
	val int64
}

func (opt withPartitionWriteSpeedBytesPerSecond) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.PartitionWriteSpeedBytesPerSecond = opt.val
}

func (opt withPartitionWriteSpeedBytesPerSecond) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.SetPartitionWriteSpeedBytesPerSecond.HasValue = true
	req.SetPartitionWriteSpeedBytesPerSecond.Value = opt.val
}

type withPartitionWriteBurstBytes struct {
	internal.InterfaceImplementation
	val int64
}

func (opt withPartitionWriteBurstBytes) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.PartitionWriteBurstBytes = opt.val
}

func (opt withPartitionWriteBurstBytes) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.SetPartitionWriteBurstBytes.HasValue = true
	req.SetPartitionWriteBurstBytes.Value = opt.val
}

type withAttributes struct {
	internal.InterfaceImplementation
	val map[string]string
}

func (opt withAttributes) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.Attributes = opt.val
}

func (opt withAttributes) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.AlterAttributes = opt.val
}

type withAddConsumers struct {
	internal.InterfaceImplementation
	val []topictypes.Consumer
}

func (opt withAddConsumers) ApplyCreateOption(request *rawtopic.CreateTopicRequest) {
	request.Consumers = make([]rawtopic.Consumer, len(opt.val))
	for i := range opt.val {
		opt.val[i].ToRaw(&request.Consumers[i])
	}
}

func (opt withAddConsumers) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.AddConsumers = make([]rawtopic.Consumer, len(opt.val))
	for i := range opt.val {
		opt.val[i].ToRaw(&req.AddConsumers[i])
	}
}

type withDropConsumers struct {
	internal.InterfaceImplementation
	consumersName []string
}

func (opt withDropConsumers) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	req.DropConsumers = opt.consumersName
}

type withConsumerWithImportant struct {
	internal.InterfaceImplementation
	name      string
	important bool
}

func (consumerImportant withConsumerWithImportant) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerImportant.name)
	req.AlterConsumers[index].SetImportant.HasValue = true
	req.AlterConsumers[index].SetImportant.Value = consumerImportant.important
}

type withConsumerWithReadFrom struct {
	internal.InterfaceImplementation
	name     string
	readFrom time.Time
}

func (consumerReadFrom withConsumerWithReadFrom) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerReadFrom.name)
	req.AlterConsumers[index].SetReadFrom.HasValue = true
	req.AlterConsumers[index].SetReadFrom.Value = consumerReadFrom.readFrom
}

type withConsumerWithSupportedCodecs struct {
	internal.InterfaceImplementation
	name   string
	codecs []topictypes.Codec
}

func (consumerCodecs withConsumerWithSupportedCodecs) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerCodecs.name)

	consumer := &req.AlterConsumers[index]
	consumer.SetSupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(consumerCodecs.codecs))
	for i, codec := range consumerCodecs.codecs {
		consumer.SetSupportedCodecs[i] = rawtopiccommon.Codec(codec)
	}
}

type withConsumerWithAttributes struct {
	internal.InterfaceImplementation
	name       string
	attributes map[string]string
}

func (consumerAttributes withConsumerWithAttributes) ApplyAlterOption(req *rawtopic.AlterTopicRequest) {
	var index int
	req.AlterConsumers, index = ensureAlterConsumer(req.AlterConsumers, consumerAttributes.name)
	req.AlterConsumers[index].AlterAttributes = consumerAttributes.attributes
}
