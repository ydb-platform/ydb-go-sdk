package rawtopic

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type CreateTopicRequest struct {
	OperationParams rawydb.OperationParams

	Path                              string
	PartitioningSettings              PartitioningSettings
	RetentionPeriod                   time.Duration
	RetentionStorageMB                int64
	SupportedCodecs                   rawtopiccommon.SupportedCodecs
	PartitionWriteSpeedBytesPerSecond int64
	PartitionWriteBurstBytes          int64
	Attributes                        map[string]string
	Consumers                         []Consumer
	MeteringMode                      MeteringMode
	MetricsLevel                      rawoptional.Uint32
}

func (req *CreateTopicRequest) ToProto() *Ydb_Topic.CreateTopicRequest {
	proto := Ydb_Topic.CreateTopicRequest_builder{
		Path:                 req.Path,
		PartitioningSettings: req.PartitioningSettings.ToProto(),
	}.Build()

	if req.RetentionPeriod != 0 {
		proto.SetRetentionPeriod(durationpb.New(req.RetentionPeriod))
	}
	proto.SetRetentionStorageMb(req.RetentionStorageMB)
	proto.SetSupportedCodecs(req.SupportedCodecs.ToProto())
	proto.SetPartitionWriteSpeedBytesPerSecond(req.PartitionWriteSpeedBytesPerSecond)
	proto.SetPartitionWriteBurstBytes(req.PartitionWriteBurstBytes)
	proto.SetAttributes(req.Attributes)

	proto.SetConsumers(make([]*Ydb_Topic.Consumer, len(req.Consumers)))
	for i := range proto.GetConsumers() {
		proto.GetConsumers()[i] = req.Consumers[i].ToProto()
	}

	proto.SetMeteringMode(Ydb_Topic.MeteringMode(req.MeteringMode))

	if req.MetricsLevel.HasValue {
		proto.SetMetricsLevel(req.MetricsLevel.Value)
	}

	return proto
}

type CreateTopicResult struct {
	Operation rawydb.Operation
}

func (r *CreateTopicResult) FromProto(proto *Ydb_Topic.CreateTopicResponse) error {
	return r.Operation.FromProtoWithStatusCheck(proto.GetOperation())
}
