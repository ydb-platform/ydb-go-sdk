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

// WithModeCallback returns a new context that triggers callback when query
// statistics become available.
//
// If the parent context already carries a stats callback, the new callback is
// chained with the existing one: both callbacks fire on every QueryStats, in
// the order they were registered (existing first, then the new one). The
// effective mode becomes max(existing mode, new mode) so the more detailed
// mode wins and previously requested detail is never silently downgraded.
//
// If callback is nil, the parent context is returned unchanged.
func WithModeCallback(ctx context.Context, mode Mode, callback func(QueryStats)) context.Context {
	if callback == nil {
		return ctx
	}

	if prev := ModeCallbackFromContext(ctx); prev != nil {
		if mode < prev.Mode {
			mode = prev.Mode
		}

		prevCallback, newCallback := prev.Callback, callback
		callback = func(qs QueryStats) {
			prevCallback(qs)
			newCallback(qs)
		}
	}

	return context.WithValue(ctx, ctxModeCallbackKey{}, &ModeCallback{
		Mode:     mode,
		Callback: callback,
	})
}

// ModeCallbackFromContext returns the ModeCallback stored in ctx by
// WithModeCallback, or nil if none was set.
func ModeCallbackFromContext(ctx context.Context) *ModeCallback {
	if v, ok := ctx.Value(ctxModeCallbackKey{}).(*ModeCallback); ok {
		return v
	}

	return nil
}

// ModeCallbackFromContextWith returns a ModeCallback that combines the stats
// callback stored in ctx (if any) with the provided callback. The effective
// mode is max(ctx mode, mode) so the more detailed mode wins. The combined
// callback first invokes the ctx callback, then the provided one.
//
// The result is ephemeral: it is NOT written back into ctx. This lets callers
// attach per-call internal callbacks (for example, an internal helper that
// reads rowsAffected from QueryStats) without leaking them into ctx and
// affecting later, unrelated calls that reuse the same ctx.
//
// callback must be non-nil. To read the ctx callback without combining, use
// ModeCallbackFromContext instead.
func ModeCallbackFromContextWith(ctx context.Context, mode Mode, callback func(QueryStats)) *ModeCallback {
	fn := callback

	if v := ModeCallbackFromContext(ctx); v != nil {
		if mode < v.Mode {
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
