package metrics

// Counter counts value
type Counter interface {
	Inc()
}

// CounterVec returns Counter from CounterVec by labels
type CounterVec interface {
	With(labels map[string]string) Counter
}
