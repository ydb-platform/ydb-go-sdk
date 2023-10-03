package metrics

// HistogramVec stores multiple dynamically created timers
type HistogramVec interface {
	With(labels map[string]string) Histogram
}

// Histogram tracks distribution of value.
type Histogram interface {
	Record(value float64)
}
