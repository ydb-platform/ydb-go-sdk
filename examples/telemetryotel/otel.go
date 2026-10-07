// Package telemetryotel demonstrates direct registration with OpenTelemetry.
package telemetryotel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

// Meter connects SDK gauge registration directly to an OTel meter.
func Meter(meter metric.Meter) telemetry.Meter {
	return func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		gauge, err := meter.Int64ObservableGauge(desc.Name,
			metric.WithDescription(desc.Description), metric.WithUnit(desc.Unit))
		if err != nil {
			return nil, err
		}
		reg, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
			return callback(ctx, func(value int64, attrs ...telemetry.Attribute) {
				attributes := make([]attribute.KeyValue, 0, len(attrs))
				for _, attr := range attrs {
					attributes = append(attributes, attribute.String(attr.Key, attr.Value))
				}
				observer.ObserveInt64(gauge, value, metric.WithAttributes(attributes...))
			})
		}, gauge)
		if err != nil {
			return nil, err
		}

		return reg.Unregister, nil
	}
}
