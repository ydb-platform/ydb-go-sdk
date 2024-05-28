package rawtopic

import (
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type CreateTopicRequest struct {
	OperationParams rawydb.OperationParams

	Path                              string
	PartitionSettings                 PartitioningSettings
	RetentionPeriod                   time.Duration
	RetentionStorageMB                int64
	SupportedCodecs                   rawtopiccommon.SupportedCodecs
	PartitionWriteSpeedBytesPerSecond int64
	PartitionWriteBurstBytes          int64
	Attributes                        map[string]string
	Consumers                         []Consumer
	MeteringMode                      MeteringMode
}

func (req *CreateTopicRequest) ToProto() *Ydb_Topic.CreateTopicRequest {
	proto := &Ydb_Topic.CreateTopicRequest{
		OperationParams:                   req.OperationParams.ToProto(),
		Path:                              req.Path,
		PartitioningSettings:              req.PartitionSettings.ToProto(),
		RetentionPeriod:                   new(durationpb.Duration),
		RetentionStorageMb:                req.RetentionStorageMB,
		SupportedCodecs:                   req.SupportedCodecs.ToProto(),
		PartitionWriteSpeedBytesPerSecond: req.PartitionWriteSpeedBytesPerSecond,
		PartitionWriteBurstBytes:          req.PartitionWriteBurstBytes,
		Attributes:                        req.Attributes,
		Consumers:                         make([]*Ydb_Topic.Consumer, len(req.Consumers)),
		MeteringMode:                      Ydb_Topic.MeteringMode(req.MeteringMode),
	}

	if req.RetentionPeriod != 0 {
		proto.RetentionPeriod = durationpb.New(req.RetentionPeriod)
	}

	for i := range proto.GetConsumers() {
		proto.Consumers[i] = req.Consumers[i].ToProto()
	}

	return proto
}

type CreateTopicResult struct {
	Operation rawydb.Operation
}

func (r *CreateTopicResult) FromProto(proto *Ydb_Topic.CreateTopicResponse) error {
	return r.Operation.FromProtoWithStatusCheck(proto.GetOperation())
}
