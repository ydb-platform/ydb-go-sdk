package topicreadercommon

import (
	"context"
	"path"
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

type ReaderMetricsConfig struct {
	Meter    telemetry.Meter
	Endpoint string
	Database string
	Name     string
}

type PartitionSessionCounter interface {
	PartitionSessionCounts() (map[string]int64, error)
}

type partitionSessionSource struct {
	provider   PartitionSessionCounter
	attributes []telemetry.Attribute
	topics     []string
	database   string
}

func RegisterPartitionSessionCount(
	cfg ReaderMetricsConfig,
	consumer string,
	selectors []*PublicReadSelector,
	provider PartitionSessionCounter,
) (telemetry.Registration, error) {
	name := cfg.Name
	if name == "" {
		name = "default"
	}
	topics := make(map[string]struct{}, len(selectors))
	database := path.Clean("/" + cfg.Database)
	for _, selector := range selectors {
		topics[metricTopicPath(database, selector.Path)] = struct{}{}
	}
	source := &partitionSessionSource{
		provider: provider,
		attributes: []telemetry.Attribute{
			{Key: "endpoint", Value: cfg.Endpoint},
			{Key: "database", Value: database},
			{Key: "consumer", Value: consumer},
			{Key: "reader.name", Value: name},
		},
		topics:   make([]string, 0, len(topics)),
		database: database,
	}
	for topic := range topics {
		source.topics = append(source.topics, topic)
	}
	sort.Strings(source.topics)

	return cfg.Meter.RegisterInt64Gauge(telemetry.Int64GaugeDescriptor{
		Descriptor: telemetry.Descriptor{
			Name:        "ydb.topic.reader.partition_session.count",
			Unit:        "{session}",
			Description: "Number of SDK-owned active topic partition sessions.",
		},
		Reduction: telemetry.GaugeSum,
	}, source)
}

func (s *partitionSessionSource) Snapshot(ctx context.Context) ([]telemetry.Int64Point, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	counts, err := s.provider.PartitionSessionCounts()
	if err != nil {
		return nil, err
	}
	normalized := make(map[string]int64, len(counts))
	for topic, count := range counts {
		normalized[metricTopicPath(s.database, topic)] += count
	}
	points := make([]telemetry.Int64Point, 0, len(s.topics))
	for _, topic := range s.topics {
		attributes := append([]telemetry.Attribute(nil), s.attributes...)
		attributes = append(attributes, telemetry.Attribute{Key: "topic", Value: topic})
		points = append(points, telemetry.Int64Point{Value: normalized[topic], Attributes: attributes})
	}

	return points, nil
}

func metricTopicPath(database, topic string) string {
	if path.IsAbs(topic) {
		return path.Clean(topic)
	}

	return path.Join(database, topic)
}
