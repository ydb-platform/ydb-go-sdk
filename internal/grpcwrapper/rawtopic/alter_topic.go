package rawtopic

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"google.golang.org/protobuf/proto"
)

type AlterTopicRequest struct {
	OperationParams rawydb.OperationParams

	Path string

	AlterPartitionSettings               AlterPartitioningSettings
	SetRetentionPeriod                   rawoptional.Duration
	SetRetentionStorageMB                rawoptional.Int64
	SetSupportedCodecs                   bool
	SetSupportedCodecsValue              rawtopiccommon.SupportedCodecs
	SetPartitionWriteSpeedBytesPerSecond rawoptional.Int64
	SetPartitionWriteBurstBytes          rawoptional.Int64
	AlterAttributes                      map[string]string
	AddConsumers                         []Consumer
	DropConsumers                        []string
	AlterConsumers                       []AlterConsumer
	SetMeteringMode                      MeteringMode
	SetMetricsLevel                      rawoptional.Uint32
	ResetMetricsLevel                    bool
}

func (req *AlterTopicRequest) ToProto() *Ydb_Topic.AlterTopicRequest {
	res := Ydb_Topic.AlterTopicRequest_builder{
		OperationParams:                      req.OperationParams.ToProto(),
		Path:                                 req.Path,
		AlterPartitioningSettings:            req.AlterPartitionSettings.ToProto(),
		SetRetentionPeriod:                   req.SetRetentionPeriod.ToProto(),
		SetRetentionStorageMb:                req.SetRetentionStorageMB.ToProto(),
		SetPartitionWriteSpeedBytesPerSecond: req.SetPartitionWriteSpeedBytesPerSecond.ToProto(),
		SetPartitionWriteBurstBytes:          req.SetPartitionWriteBurstBytes.ToProto(),
		AlterAttributes:                      req.AlterAttributes,
		SetMeteringMode:                      Ydb_Topic.MeteringMode(req.SetMeteringMode),
	}.Build()

	if req.SetSupportedCodecs {
		res.SetSetSupportedCodecs(req.SetSupportedCodecsValue.ToProto())
	}

	if req.SetMetricsLevel.HasValue {
		res.SetSetMetricsLevel(req.SetMetricsLevel.Value)
	}
	if req.ResetMetricsLevel {
		res.SetResetMetricsLevel(&emptypb.Empty{})
	}

	consumers := make([]*Ydb_Topic.Consumer, len(req.AddConsumers))
	for i := range req.AddConsumers {
		consumers[i] = req.AddConsumers[i].ToProto()
	}
	res.SetAddConsumers(consumers)

	res.SetDropConsumers(req.DropConsumers)

	alterConsumers := make([]*Ydb_Topic.AlterConsumer, len(req.AlterConsumers))
	for i := range req.AlterConsumers {
		alterConsumers[i] = req.AlterConsumers[i].ToProto()
	}
	res.SetAlterConsumers(alterConsumers)

	return res
}

type AlterTopicResult struct {
	Operation rawydb.Operation
}

func (r *AlterTopicResult) FromProto(proto *Ydb_Topic.AlterTopicResponse) error {
	return r.Operation.FromProtoWithStatusCheck(proto.GetOperation())
}

type AlterConsumer struct {
	Name                    string
	SetImportant            rawoptional.Bool
	SetReadFrom             rawoptional.Time
	SetSupportedCodecs      rawtopiccommon.SupportedCodecs
	SetAvailabilityPeriod   rawoptional.Duration
	ResetAvailabilityPeriod bool
	AlterAttributes         map[string]string
}

func (c *AlterConsumer) ToProto() *Ydb_Topic.AlterConsumer {
	res := Ydb_Topic.AlterConsumer_builder{
		Name:               c.Name,
		SetImportant:       c.SetImportant.ToProto(),
		SetReadFrom:        c.SetReadFrom.ToProto(),
		SetSupportedCodecs: c.SetSupportedCodecs.ToProto(),
		AlterAttributes:    c.AlterAttributes,
	}.Build()

	if c.SetAvailabilityPeriod.HasValue {
		res.SetSetAvailabilityPeriod(proto.ValueOrDefault(c.SetAvailabilityPeriod.ToProto()))
	}
	if c.ResetAvailabilityPeriod {
		res.SetResetAvailabilityPeriod(&emptypb.Empty{})
	}

	return res
}
