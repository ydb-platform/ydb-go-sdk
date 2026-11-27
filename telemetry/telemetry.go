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

// Int64GaugeCallback observes current values without I/O or SDK mutation.
// It must honor cancellation and support concurrent calls. Observations are
// valid for this invocation only; the backend owns collection and aggregation.
// Different resources can observe the same attribute set, and the backend must
// sum their observations within one collection.
type Int64GaugeCallback func(ctx context.Context, observe func(int64, ...Attribute)) error

// Meter directly registers a gauge and its callback with a metrics backend.
// On success it must return a non-nil, idempotent unregister function; a nil
// function is a registration error. On failure it must not retain the callback.
// Closing a resource disables its callback and schedules unregister without
// waiting, so Close can run during collection. A collection already observing
// the resource may finish. Unregister can run concurrently with collection and
// registration; the backend owns synchronization and must report cleanup errors
// itself if needed, since they are not returned by Close.
// Nil disables observable metrics.
type Meter func(Descriptor, Int64GaugeCallback) (func() error, error)
