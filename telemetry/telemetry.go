// Package telemetry defines backend-independent observable gauges.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
package telemetry

import "context"

type Descriptor struct {
	Name        string
	Unit        string
	Description string
}

type Attribute struct {
	Key   string
	Value string
}

type GaugeReduction uint8

const (
	GaugeSum GaugeReduction = iota + 1
	GaugeMax
)

type Int64GaugeDescriptor struct {
	Descriptor

	Reduction GaugeReduction
}

type Int64Point struct {
	Value      int64
	Attributes []Attribute
}

// Int64GaugeSource returns an owned snapshot, without I/O or SDK mutation.
// Snapshot must honor cancellation and permit concurrent calls.
type Int64GaugeSource interface {
	Snapshot(ctx context.Context) ([]Int64Point, error)
}

// Registration removes a source and waits for collections already using it.
// Close is idempotent. Even when its context expires, the source is detached;
// a later Close can wait for quiescence. Collections begun before Close may
// return their earlier observations. Later collections cannot use the source.
type Registration interface {
	Close(ctx context.Context) error
}

// Meter registers observable gauges independently of diagnostic tracing.
// Equal descriptors and attribute sets must be reduced before export.
// Registration failure must not retain the source.
type Meter interface {
	RegisterInt64Gauge(descriptor Int64GaugeDescriptor, source Int64GaugeSource) (Registration, error)
}

type Int64Metric struct {
	Descriptor Int64GaugeDescriptor
	Points     []Int64Point
}
