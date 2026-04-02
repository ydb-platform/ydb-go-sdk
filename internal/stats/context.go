package stats

import (
	"context"
)

type ctxModeCallbackKey struct{}

// ModeCallback represents a callback configuration for query statistics.
type ModeCallback struct {
	Mode     Mode
	Callback func(QueryStats)
}

// WithModeCallback returns a new context with the specified statistics mode and callback function.
// The callback will be invoked when query statistics become available.
func WithModeCallback(ctx context.Context, mode Mode, callback func(QueryStats)) context.Context {
	return context.WithValue(ctx, ctxModeCallbackKey{}, &ModeCallback{
		Mode:     mode,
		Callback: callback,
	})
}

// ModeCallbackFromContext extracts the ModeCallback from the context.
// Returns the ModeCallback if present in the context, otherwise returns nil.
func ModeCallbackFromContext(ctx context.Context) *ModeCallback {
	if v, ok := ctx.Value(ctxModeCallbackKey{}).(*ModeCallback); ok {
		return v
	}

	return nil
}

// ModeCallbackFromContextWith extracts the ModeCallback from the context,
// appending a configuration to the context mode.
func ModeCallbackFromContextWith(ctx context.Context, mode Mode, callback func(QueryStats)) *ModeCallback {
	fn := callback

	if v, ok := ctx.Value(ctxModeCallbackKey{}).(*ModeCallback); ok {
		if mode < v.Mode { // greater mode wins
			mode = v.Mode
		}

		fn = func(qs QueryStats) {
			v.Callback(qs)
			callback(qs)
		}
	}

	return &ModeCallback{
		Mode:     mode,
		Callback: fn,
	}
}
