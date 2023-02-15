package rawtopic

import (
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/clone"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawscheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type DescribeTopicRequest struct {
	OperationParams rawydb.OperationParams
	Path            string
}

func (req *DescribeTopicRequest) ToProto() *Ydb_Topic.DescribeTopicRequest {
	return &Ydb_Topic.DescribeTopicRequest{
		OperationParams: req.OperationParams.ToProto(),
		Path:            req.Path,
	}
}

type DescribeTopicResult struct {
	Operation rawydb.Operation

	Self                              rawscheme.Entry
	PartitioningSettings              PartitioningSettings
	Partitions                        []PartitionInfo
	RetentionPeriod                   time.Duration
	RetentionStorageMB                int64
	SupportedCodecs                   rawtopiccommon.SupportedCodecs
	PartitionWriteSpeedBytesPerSecond int64
	PartitionWriteBurstBytes          int64
	Attributes                        map[string]string
	Consumers                         []Consumer
	MeteringMode                      MeteringMode
}

func (res *DescribeTopicResult) FromProto(protoResponse *Ydb_Topic.DescribeTopicResponse) error {
	if err := res.Operation.FromProtoWithStatusCheck(protoResponse.Operation); err != nil {
		return nil
	}

	protoResult := &Ydb_Topic.DescribeTopicResult{}
	if err := protoResponse.GetOperation().GetResult().UnmarshalTo(protoResult); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: describe topic result failed on unmarshal grpc result: %w", err))
	}

	if err := res.Self.FromProto(protoResult.Self); err != nil {
		return err
	}

	if err := res.PartitioningSettings.FromProto(protoResult.PartitioningSettings); err != nil {
		return err
	}

	protoPartitions := protoResult.GetPartitions()
	res.Partitions = make([]PartitionInfo, len(protoPartitions))
	for i, protoPartition := range protoPartitions {
		res.Partitions[i].mustFromProto(protoPartition)
	}

	res.RetentionPeriod = protoResult.GetRetentionPeriod().AsDuration()
	res.RetentionStorageMB = protoResult.GetRetentionStorageMb()

	for _, v := range protoResult.GetSupportedCodecs().GetCodecs() {
		res.SupportedCodecs = append(res.SupportedCodecs, rawtopiccommon.Codec(v))
	}

	res.PartitionWriteSpeedBytesPerSecond = protoResult.PartitionWriteSpeedBytesPerSecond
	res.PartitionWriteBurstBytes = protoResult.PartitionWriteBurstBytes

	res.Attributes = protoResult.Attributes

	res.Consumers = make([]Consumer, len(protoResult.Consumers))
	for i := range res.Consumers {
		res.Consumers[i].MustFromProto(protoResult.Consumers[i])
	}

	res.MeteringMode = MeteringMode(protoResult.MeteringMode)

	return nil
}

type PartitionInfo struct {
	PartitionID        int64
	Active             bool
	ChildPartitionIDs  []int64
	ParentPartitionIDs []int64
}

func (pi *PartitionInfo) mustFromProto(proto *Ydb_Topic.DescribeTopicResult_PartitionInfo) {
	pi.PartitionID = proto.GetPartitionId()
	pi.Active = proto.GetActive()

	pi.ChildPartitionIDs = clone.Int64Slice(proto.GetChildPartitionIds())
	pi.ParentPartitionIDs = clone.Int64Slice(proto.GetParentPartitionIds())
}
