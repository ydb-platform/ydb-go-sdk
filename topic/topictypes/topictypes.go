package topictypes

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

// Codec
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Codec int

const (
	CodecRaw  = Codec(rawtopiccommon.CodecRaw)
	CodecGzip = Codec(rawtopiccommon.CodecGzip)
	CodecLzop = Codec(rawtopiccommon.CodecLzop)
	CodecZstd = Codec(rawtopiccommon.CodecZstd)

	CodecCustomerFirst = Codec(rawtopiccommon.CodecCustomerFirst)
	CodecCustomerEnd   = Codec(rawtopiccommon.CodecCustomerEnd) // last allowed custom codec id is CodecCustomerEnd-1
)

// Consumer
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs []Codec
	ReadFrom        time.Time
	Attributes      map[string]string
}

// ToRaw
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
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

// FromRaw
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
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

// PartitionSettings
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PartitionSettings struct {
	MinActivePartitions int64
	PartitionCountLimit int64
}

// ToRaw
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (s *PartitionSettings) ToRaw(raw *rawtopic.PartitioningSettings) {
	raw.MinActivePartitions = s.MinActivePartitions
	raw.PartitionCountLimit = s.PartitionCountLimit
}

// FromRaw
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (s *PartitionSettings) FromRaw(raw *rawtopic.PartitioningSettings) {
	s.MinActivePartitions = raw.MinActivePartitions
	s.PartitionCountLimit = raw.PartitionCountLimit
}

// TopicDescription
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type TopicDescription struct {
	Path                              string
	PartitionSettings                 PartitionSettings
	Consumers                         []Consumer
	SupportedCodecs                   []Codec
	RetentionPeriod                   time.Duration
	PartitionWriteBurstBytes          int64
	PartitionWriteSpeedBytesPerSecond int64
	Attributes                        map[string]string
}

// FromRaw
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func (d *TopicDescription) FromRaw(raw *rawtopic.DescribeTopicResult) {
	d.Path = raw.Self.Name
	d.PartitionSettings.FromRaw(&raw.PartitioningSettings)

	d.Consumers = make([]Consumer, len(raw.Consumers))
	for i := 0; i < len(raw.Consumers); i++ {
		d.Consumers[i].FromRaw(&raw.Consumers[i])
	}

	d.SupportedCodecs = make([]Codec, len(raw.SupportedCodecs))
	for i := 0; i < len(raw.SupportedCodecs); i++ {
		d.SupportedCodecs[i] = Codec(raw.SupportedCodecs[i])
	}

	d.PartitionWriteSpeedBytesPerSecond = raw.PartitionWriteSpeedBytesPerSecond
	d.PartitionWriteBurstBytes = raw.PartitionWriteBurstBytes

	d.Attributes = make(map[string]string)
	for k, v := range raw.Attributes {
		d.Attributes[k] = v
	}
}
