package topictypes

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/clone"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

// Codec code for use in topics
// Allow to use custom values in interval [10000,20000)
type Codec int32

const (
	CodecRaw  = Codec(rawtopiccommon.CodecRaw)
	CodecGzip = Codec(rawtopiccommon.CodecGzip)

	// CodecLzop not supported by default, customer need provide own codec library
	CodecLzop = Codec(rawtopiccommon.CodecLzop)

	// CodecZstd not supported by default, customer need provide own codec library
	CodecZstd = Codec(rawtopiccommon.CodecZstd)

	CodecCustomerFirst = Codec(rawtopiccommon.CodecCustomerFirst)
	CodecCustomerEnd   = Codec(rawtopiccommon.CodecCustomerEnd) // last allowed custom codec id is CodecCustomerEnd-1
)

func (c Codec) ToRaw(r *rawtopiccommon.Codec) {
	*r = rawtopiccommon.Codec(c)
}

// Consumer contains info about topic consumer
type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs []Codec
	ReadFrom        time.Time
	Attributes      map[string]string
}

// ToRaw public format to internal. Used internally only.
func (c *Consumer) ToRaw(raw *rawtopic.Consumer) {
	raw.Name = c.Name
	raw.Important = c.Important
	raw.Attributes = c.Attributes

	raw.SupportedCodecs = make(rawtopiccommon.SupportedCodecs, len(c.SupportedCodecs))
	for index, codec := range c.SupportedCodecs {
		raw.SupportedCodecs[index] = rawtopiccommon.Codec(codec)
	}

	if !c.ReadFrom.IsZero() {
		raw.ReadFrom.HasValue = true
		raw.ReadFrom.Value = c.ReadFrom
	}
	raw.Attributes = c.Attributes
}

// FromRaw convert internal format to public. Used internally only.
func (c *Consumer) FromRaw(raw *rawtopic.Consumer) {
	c.Attributes = raw.Attributes
	c.Important = raw.Important
	c.Name = raw.Name

	c.SupportedCodecs = make([]Codec, len(raw.SupportedCodecs))
	for index, codec := range raw.SupportedCodecs {
		c.SupportedCodecs[index] = Codec(codec)
	}

	if raw.ReadFrom.HasValue {
		c.ReadFrom = raw.ReadFrom.Value
	}
}

// MeteringMode mode of topic's metering. Used for serverless installations.
type MeteringMode int

const (
	MeteringModeUnspecified      = MeteringMode(rawtopic.MeteringModeUnspecified)
	MeteringModeReservedCapacity = MeteringMode(rawtopic.MeteringModeReservedCapacity)
	MeteringModeRequestUnits     = MeteringMode(rawtopic.MeteringModeRequestUnits)
)

// FromRaw convert from internal format to public. Used internally only.
func (m *MeteringMode) FromRaw(raw rawtopic.MeteringMode) {
	*m = MeteringMode(raw)
}

// ToRaw convert from public format to internal. Used internally only.
func (m *MeteringMode) ToRaw(raw *rawtopic.MeteringMode) {
	*raw = rawtopic.MeteringMode(*m)
}

// PartitionSettings settings of partitions
type PartitionSettings struct {
	MinActivePartitions int64
	PartitionCountLimit int64
}

// ToRaw convert public format to internal. Used internally only.
func (s *PartitionSettings) ToRaw(raw *rawtopic.PartitioningSettings) {
	raw.MinActivePartitions = s.MinActivePartitions
	raw.PartitionCountLimit = s.PartitionCountLimit
}

// FromRaw convert internal format to public. Used internally only.
func (s *PartitionSettings) FromRaw(raw *rawtopic.PartitioningSettings) {
	s.MinActivePartitions = raw.MinActivePartitions
	s.PartitionCountLimit = raw.PartitionCountLimit
}

// TopicDescription contains info about topic.
type TopicDescription struct {
	Path                              string
	PartitionSettings                 PartitionSettings
	Partitions                        []PartitionInfo
	RetentionPeriod                   time.Duration
	RetentionStorageMB                int64
	SupportedCodecs                   []Codec
	PartitionWriteBurstBytes          int64
	PartitionWriteSpeedBytesPerSecond int64
	Attributes                        map[string]string
	Consumers                         []Consumer
	MeteringMode                      MeteringMode
}

// FromRaw convert from public format to internal. Used internally only.
func (d *TopicDescription) FromRaw(raw *rawtopic.DescribeTopicResult) {
	d.Path = raw.Self.Name
	d.PartitionSettings.FromRaw(&raw.PartitioningSettings)

	d.Partitions = make([]PartitionInfo, len(raw.Partitions))
	for i := range raw.Partitions {
		d.Partitions[i].FromRaw(&raw.Partitions[i])
	}

	d.RetentionPeriod = raw.RetentionPeriod
	d.RetentionStorageMB = raw.RetentionStorageMB

	d.SupportedCodecs = make([]Codec, len(raw.SupportedCodecs))
	for i := 0; i < len(raw.SupportedCodecs); i++ {
		d.SupportedCodecs[i] = Codec(raw.SupportedCodecs[i])
	}

	d.PartitionWriteSpeedBytesPerSecond = raw.PartitionWriteSpeedBytesPerSecond
	d.PartitionWriteBurstBytes = raw.PartitionWriteBurstBytes

	d.RetentionPeriod = raw.RetentionPeriod
	d.RetentionStorageMB = raw.RetentionStorageMB

	d.Attributes = make(map[string]string)
	for k, v := range raw.Attributes {
		d.Attributes[k] = v
	}

	d.Consumers = make([]Consumer, len(raw.Consumers))
	for i := 0; i < len(raw.Consumers); i++ {
		d.Consumers[i].FromRaw(&raw.Consumers[i])
	}

	d.MeteringMode.FromRaw(raw.MeteringMode)
}

// PartitionInfo contains info about partition.
type PartitionInfo struct {
	PartitionID        int64
	Active             bool
	ChildPartitionIDs  []int64
	ParentPartitionIDs []int64
}

// FromRaw convert from internal format to public. Used internally only.
func (p *PartitionInfo) FromRaw(raw *rawtopic.PartitionInfo) {
	p.PartitionID = raw.PartitionID
	p.Active = raw.Active

	p.ChildPartitionIDs = clone.Int64Slice(raw.ChildPartitionIDs)
	p.ParentPartitionIDs = clone.Int64Slice(raw.ParentPartitionIDs)
}
