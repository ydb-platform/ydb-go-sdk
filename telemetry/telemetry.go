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
type Int64GaugeCallback func(ctx context.Context, observe func(int64, ...Attribute)) error

// Meter directly registers a gauge and its callback with a metrics backend.
// On success it returns a non-nil, idempotent unregister function. On failure
// it must not retain the callback. The backend owns unregister synchronization.
// Nil disables observable metrics.
type Meter func(Descriptor, Int64GaugeCallback) (func() error, error)
