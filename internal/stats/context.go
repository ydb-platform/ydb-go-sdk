package stats

import (
	"context"
)

type ctxModeCallbackKey struct{}

type ModeCallback struct {
	Mode     Mode
	Callback func(QueryStats)
}

func WithModeCallback(ctx context.Context, mode Mode, callback func(QueryStats)) context.Context {
	return context.WithValue(ctx, ctxModeCallbackKey{}, &ModeCallback{
		Mode:     mode,
		Callback: callback,
	})
}

func ModeCallbackFromContext(ctx context.Context) *ModeCallback {
	if v, ok := ctx.Value(ctxModeCallbackKey{}).(*ModeCallback); ok {
		return v
	}

	return nil
}
