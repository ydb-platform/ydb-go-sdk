package metrics

// Registry is interface for metrics registry
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type Registry interface {
	// CounterVec returns CounterVec by name, subsystem and labels
	// If counter by args already created - return counter from cache
	// If counter by args nothing - create and return newest counter
	CounterVec(name string, labelNames ...string) CounterVec

	// GaugeVec returns GaugeVec by name, subsystem and labels
	// If gauge by args already created - return gauge from cache
	// If gauge by args nothing - create and return newest gauge
	GaugeVec(name string, labelNames ...string) GaugeVec

	// TimerVec returns TimerVec by name, subsystem and labels
	// If timer by args already created - return timer from cache
	// If timer by args nothing - create and return newest timer
	TimerVec(name string, labelNames ...string) TimerVec

	// HistogramVec returns HistogramVec by name, subsystem and labels
	// If histogram by args already created - return histogram from cache
	// If histogram by args nothing - create and return newest histogram
	HistogramVec(name string, buckets []float64, labelNames ...string) HistogramVec
}

// RegistryWithDescriptors is an optional Registry capability for creating a
// counter with an explicit instrument name and unit.
//
// The name passed to CounterVecWithDescriptor is used as a complete instrument
// name by implementations that support this capability. Registries that do
// not implement RegistryWithDescriptors continue to use Registry.CounterVec.
// This capability is intentionally additive so existing Registry and Config
// implementations remain valid.
type RegistryWithDescriptors interface {
	CounterVecWithDescriptor(name, unit string, labelNames ...string) CounterVec
}
