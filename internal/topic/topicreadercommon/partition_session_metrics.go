package topicreadercommon

import (
	"context"
	"path"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

type ReaderMetricsConfig struct {
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
) (func() error, error) {
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
		{Key: "consumer", Value: consumer},
		{Key: "reader.name", Value: name},
	}

	return cfg.Meter(telemetry.Descriptor{
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
			observe(normalized[topic], append(attributes, telemetry.Attribute{Key: "topic", Value: topic})...)
		}

		return nil
	})
}

func metricTopicPath(database, topic string) string {
	if path.IsAbs(topic) {
		return path.Clean(topic)
	}

	return path.Join(database, topic)
}
