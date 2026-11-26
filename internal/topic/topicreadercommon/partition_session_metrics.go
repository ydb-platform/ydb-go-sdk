package topicreadercommon

import (
	"context"
	"errors"
	"path"
	"slices"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

var errNilMetricUnregister = errors.New("ydb: telemetry Meter returned a nil unregister function")

type ReaderMetricsConfig struct {
	config.Common

	Meter    telemetry.Meter
	Endpoint string
	Database string
	Name     string
}

func RegisterPartitionSessionCount(
	cfg ReaderMetricsConfig,
	consumer string,
	selectors []*PublicReadSelector,
	counts func() (map[string]int64, error),
) (func(), error) {
	name := cfg.Name
	if name == "" {
		name = "default"
	}
	topics := make(map[string]struct{}, len(selectors))
	database := path.Clean("/" + cfg.Database)
	for _, selector := range selectors {
		topics[metricTopicPath(database, selector.Path)] = struct{}{}
	}
	attributes := []telemetry.Attribute{
		{Key: "endpoint", Value: cfg.Endpoint},
		{Key: "database", Value: database},
		{Key: "reader.name", Value: name},
	}
	if consumer != "" {
		attributes = append(attributes, telemetry.Attribute{Key: "consumer", Value: consumer})
	}
	// Every observation appends its topic, so spare capacity must not be shared.
	attributes = slices.Clip(attributes)

	return registerGauge(cfg, telemetry.Descriptor{
		Name:        "ydb.topic.reader.partition_session.count",
		Unit:        "{session}",
		Description: "Number of SDK-owned active topic partition sessions.",
	}, func(ctx context.Context, observe func(int64, ...telemetry.Attribute)) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := counts()
		if err != nil {
			return err
		}
		normalized := make(map[string]int64, len(current))
		for topic, count := range current {
			normalized[metricTopicPath(database, topic)] += count
		}
		for topic := range topics {
			if err := ctx.Err(); err != nil {
				return err
			}
			observe(normalized[topic], append(attributes, telemetry.Attribute{Key: "topic", Value: topic})...)
		}

		return nil
	})
}

func registerGauge(
	cfg ReaderMetricsConfig, desc telemetry.Descriptor, collect telemetry.Int64GaugeCallback,
) (func(), error) {
	var collector atomic.Pointer[telemetry.Int64GaugeCallback]
	collector.Store(&collect)
	unregister, err := cfg.Meter(desc, func(ctx context.Context, observe func(int64, ...telemetry.Attribute)) error {
		current := collector.Load()
		if current == nil {
			return nil
		}

		return (*current)(ctx, observe)
	})
	if err != nil {
		collector.Store(nil)

		return nil, err
	}
	if unregister == nil {
		collector.Store(nil)

		return nil, errNilMetricUnregister
	}

	panicCallback := cfg.PanicCallback()

	return func() {
		if collector.Swap(nil) != nil {
			// A collecting backend may wait for Close before it unlocks unregister.
			go func() {
				if panicCallback != nil {
					defer func() {
						if e := recover(); e != nil {
							panicCallback(e)
						}
					}()
				}
				_ = unregister() // Deferred cleanup errors are reported by the backend.
			}()
		}
	}, nil
}

func metricTopicPath(database, topic string) string {
	if path.IsAbs(topic) {
		return path.Clean(topic)
	}

	return path.Join(database, topic)
}
