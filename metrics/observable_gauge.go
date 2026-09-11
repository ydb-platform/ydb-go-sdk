package metrics

import "context"

// ObservableGaugeCallback is called by an ObservableGaugeVec during metric
// collection. The callback may emit zero or more observations through observe.
//
// Implementations may call callbacks concurrently and must pass a context
// whose cancellation and deadline the callback honors. The callback must be
// safe for reentrant and concurrent calls and should return context errors
// promptly. The labels map passed to observe is consumed synchronously;
// callers must not mutate it while the call is in progress. observe is valid
// only during the callback invocation and may be called concurrently by the
// callback.
type ObservableGaugeCallback func(
	ctx context.Context,
	observe func(value float64, labels map[string]string),
) error

// ObservableGaugeVec registers callbacks for a float64 observable gauge.
//
// Register returns an idempotent, concurrency-safe unregister function. A
// successful unregister prevents future callback invocations, but may return
// while a callback already in progress is finishing. Register errors leave no
// active callback. Callback errors are reported by the registry implementation
// for that collection and do not implicitly unregister the callback.
// Implementations must preserve unique observations for an instrument and
// label set; the SDK performs topic-specific aggregation before invoking the
// callback. Registrations belonging to independent Registry/Config owners
// are not aggregated with each other.
type ObservableGaugeVec interface {
	Register(callback ObservableGaugeCallback) (unregister func() error, err error)
}
