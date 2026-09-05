package metrics

// Counter counts value
type Counter interface {
	Inc()
}

// CounterAdder is an optional capability for counters that can add a batch of
// values in one operation.
//
// Implementations of Counter are not required to implement CounterAdder. Code
// using a CounterAdder must retain a fallback to Counter.Inc for compatibility
// with existing registry implementations.
type CounterAdder interface {
	Counter
	Add(delta int64)
}

// CounterVec returns Counter from CounterVec by labels
type CounterVec interface {
	With(labels map[string]string) Counter
}
