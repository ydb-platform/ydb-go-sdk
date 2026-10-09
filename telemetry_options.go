package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

// WithMeter directly registers observable gauge callbacks with the backend.
// Nil disables them. Child drivers inherit the registration function.
// Readers/listeners own their callbacks: close them before closing the driver.
// The application owns the backend and its collection/export lifecycle.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithMeter(meter telemetry.Meter) Option {
	return func(_ context.Context, d *Driver) error {
		d.meter = meter

		return nil
	}
}
