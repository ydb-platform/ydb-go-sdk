package rawtopic

import (
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errUnexpectedNilPartitioningSettings = xerrors.Wrap(errors.New("ydb: unexpected nil partitioning settings"))

type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs rawtopiccommon.SupportedCodecs
	ReadFrom        rawoptional.Time
	Attributes      map[string]string
}

func (c *Consumer) MustFromProto(consumer *Ydb_Topic.Consumer) {
	c.Name = consumer.GetName()
	c.Important = consumer.GetImportant()
	c.Attributes = consumer.GetAttributes()
	c.ReadFrom.MustFromProto(consumer.GetReadFrom())
	c.SupportedCodecs.MustFromProto(consumer.GetSupportedCodecs())
}

func (c *Consumer) ToProto() *Ydb_Topic.Consumer {
	return &Ydb_Topic.Consumer{
		Name:            c.Name,
		Important:       c.Important,
		ReadFrom:        c.ReadFrom.ToProto(),
		SupportedCodecs: c.SupportedCodecs.ToProto(),
		Attributes:      c.Attributes,
	}
}

type MeteringMode int

const (
	MeteringModeUnspecified      = MeteringMode(Ydb_Topic.MeteringMode_METERING_MODE_UNSPECIFIED)
	MeteringModeReservedCapacity = MeteringMode(Ydb_Topic.MeteringMode_METERING_MODE_RESERVED_CAPACITY)
	MeteringModeRequestUnits     = MeteringMode(Ydb_Topic.MeteringMode_METERING_MODE_REQUEST_UNITS)
)

type PartitioningSettings struct {
	MinActivePartitions int64
	PartitionCountLimit int64
}

func (s *PartitioningSettings) FromProto(proto *Ydb_Topic.PartitioningSettings) error {
	if proto == nil {
		return xerrors.WithStackTrace(errUnexpectedNilPartitioningSettings)
	}

	s.MinActivePartitions = proto.GetMinActivePartitions()
	s.PartitionCountLimit = proto.GetPartitionCountLimit()

	return nil
}

func (s *PartitioningSettings) ToProto() *Ydb_Topic.PartitioningSettings {
	return &Ydb_Topic.PartitioningSettings{
		MinActivePartitions: s.MinActivePartitions,
		PartitionCountLimit: s.PartitionCountLimit,
	}
}

type AlterPartitioningSettings struct {
	SetMinActivePartitions rawoptional.Int64
	SetPartitionCountLimit rawoptional.Int64
}

func (s *AlterPartitioningSettings) ToProto() *Ydb_Topic.AlterPartitioningSettings {
	return &Ydb_Topic.AlterPartitioningSettings{
		SetMinActivePartitions: s.SetMinActivePartitions.ToProto(),
		SetPartitionCountLimit: s.SetPartitionCountLimit.ToProto(),
	}
}
