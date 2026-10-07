package ydb_test

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

type observedMetric struct {
	Descriptor telemetry.Descriptor
	Points     []observedPoint
}

type observedPoint struct {
	Value      int64
	Attributes []telemetry.Attribute
}

func newMetricMeter() (telemetry.Meter, func(context.Context) ([]observedMetric, error)) {
	type registration struct {
		descriptor telemetry.Descriptor
		callback   telemetry.Int64GaugeCallback
	}
	registrations := make(map[int]registration)
	next := 0
	meter := func(desc telemetry.Descriptor, callback telemetry.Int64GaugeCallback) (func() error, error) {
		next++
		id := next
		registrations[id] = registration{descriptor: desc, callback: callback}

		return func() error {
			delete(registrations, id)

			return nil
		}, nil
	}
	collect := func(ctx context.Context) ([]observedMetric, error) {
		var metrics []observedMetric
		for _, reg := range registrations {
			data := observedMetric{Descriptor: reg.descriptor}
			err := reg.callback(ctx, func(value int64, attrs ...telemetry.Attribute) {
				data.Points = append(data.Points, observedPoint{
					Value: value, Attributes: append([]telemetry.Attribute(nil), attrs...),
				})
			})
			if err != nil {
				return nil, err
			}
			metrics = append(metrics, data)
		}

		return metrics, nil
	}

	return meter, collect
}
