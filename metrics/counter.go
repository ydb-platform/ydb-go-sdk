package metrics

// Counter counts values.
//
// Implementations may additionally expose Add(int64) for efficient batched
// increments; Add is an optional capability and is not part of this interface.
type Counter interface {
	Inc()
}

// CounterVec returns Counter from CounterVec by labels
type CounterVec interface {
	With(labels map[string]string) Counter
}
