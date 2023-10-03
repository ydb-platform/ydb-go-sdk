package metrics

import "time"

// TimerVec stores multiple dynamically created timers
type TimerVec interface {
	With(labels map[string]string) Timer
}

// Timer tracks distribution of value.
type Timer interface {
	Record(value time.Duration)
}
