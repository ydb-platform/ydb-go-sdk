package metrics

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	topicReaderMessageAgeName      = "ydb.topic.reader.local_buffer.message_age.max"
	topicReaderMessageAgeUnit      = "s"
	topicReaderCommitOffsetLagName = "ydb.topic.reader.commit_offset.lag.max"
	topicReaderCommitOffsetLagUnit = "1"
	topicReaderSessionCountName    = "ydb.topic.reader.partition_session.count"
	topicReaderSessionCountUnit    = "{session}"
)

type topicObservableKind uint8

const (
	topicObservableMessageAge topicObservableKind = iota
	topicObservableCommitOffsetLag
	topicObservableSessionCount
)

type topicObservableMetric struct {
	vec  ObservableGaugeVec
	kind topicObservableKind
}

type topicObservableCollector struct {
	mu sync.Mutex
	// opMu serializes adapter Register/Unregister calls. Neither call is made
	// while mu is held because an adapter may invoke callbacks synchronously.
	opMu sync.Mutex

	sources map[*topicObservableSource]struct{}
	details trace.Details

	metrics          []topicObservableMetric
	registrations    []func() error
	registered       bool
	nextGeneration   uint64
	activeGeneration uint64
}

type topicObservableSource struct {
	mu sync.Mutex

	source trace.TopicReaderMetricsSource
	labels map[string]string
	age    bool
	lag    bool
	count  bool
	active bool
}

type topicObservableValue struct {
	value  float64
	labels map[string]string
}

type topicObservableLabels struct {
	endpoint   string
	database   string
	consumer   string
	readerName string
}

func setupTopicReaderObservableMetrics(t *trace.Topic, config Config) {
	details := config.Details()
	registry, ok := config.(RegistryWithObservableGaugeDescriptors)
	if !ok {
		return
	}

	collector := newTopicObservableCollector(registry, details)
	if collector == nil {
		return
	}

	t.OnReaderMetricsSource = collector.start
}

func newTopicObservableCollector(
	registry RegistryWithObservableGaugeDescriptors,
	details trace.Details,
) *topicObservableCollector {
	collector := &topicObservableCollector{
		sources: make(map[*topicObservableSource]struct{}),
		details: details,
	}

	if details&(trace.TopicReaderMessageEvents|trace.TopicListenerStreamEvents) != 0 {
		collector.metrics = append(collector.metrics, topicObservableMetric{
			vec: registry.ObservableGaugeVecWithDescriptor(
				topicReaderMessageAgeName,
				topicReaderMessageAgeUnit,
				topicStreamLabels...,
			),
			kind: topicObservableMessageAge,
		})
	}
	if details&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) != 0 {
		collector.metrics = append(collector.metrics, topicObservableMetric{
			vec: registry.ObservableGaugeVecWithDescriptor(
				topicReaderCommitOffsetLagName,
				topicReaderCommitOffsetLagUnit,
				topicStreamLabels...,
			),
			kind: topicObservableCommitOffsetLag,
		})
	}
	if details&(trace.TopicReaderStreamEvents|trace.TopicListenerStreamEvents) != 0 {
		collector.metrics = append(collector.metrics, topicObservableMetric{
			vec: registry.ObservableGaugeVecWithDescriptor(
				topicReaderSessionCountName,
				topicReaderSessionCountUnit,
				topicStreamLabels...,
			),
			kind: topicObservableSessionCount,
		})
	}

	if len(collector.metrics) == 0 {
		return nil
	}

	return collector
}

func (c *topicObservableCollector) start(
	info trace.TopicReaderMetricsSourceStartInfo,
) func(trace.TopicReaderMetricsSourceDoneInfo) {
	age, lag, count := c.enabledKinds(info.Listener)
	if info.Source == nil || (!age && !lag && !count) {
		return func(trace.TopicReaderMetricsSourceDoneInfo) {}
	}

	source := &topicObservableSource{
		source: info.Source,
		labels: streamLabels(info.Endpoint, info.Database, info.Consumer, info.ReaderName),
		age:    age,
		lag:    lag,
		count:  count,
		active: true,
	}

	c.addSource(source)
	c.reconcile()

	var once sync.Once

	return func(trace.TopicReaderMetricsSourceDoneInfo) {
		once.Do(func() {
			c.remove(source)
		})
	}
}

func (c *topicObservableCollector) enabledKinds(listener bool) (age, lag, count bool) {
	if listener {
		age = c.details&trace.TopicListenerStreamEvents != 0
		lag = c.details&trace.TopicListenerStreamEvents != 0
		count = c.details&trace.TopicListenerStreamEvents != 0
	} else {
		age = c.details&trace.TopicReaderMessageEvents != 0
		lag = c.details&trace.TopicReaderStreamEvents != 0
		count = c.details&trace.TopicReaderStreamEvents != 0
	}

	return age, lag, count
}

func (c *topicObservableCollector) remove(source *topicObservableSource) {
	c.removeSource(source)

	source.close()
	c.reconcile()
}

func (c *topicObservableCollector) reconcile() {
	c.opMu.Lock()
	defer c.opMu.Unlock()

	for {
		action := c.reconcileAction()
		switch action.kind {
		case topicObservableUnregister:
			unregisterTopicObservableMetrics(action.registrations)
		case topicObservableRegister:
			registrations, ok := c.registerTopicObservableMetrics(action.generation)
			if !ok {
				return
			}
			if c.commitRegistration(action.generation, registrations) {
				return
			}
			unregisterTopicObservableMetrics(registrations)
		default:
			return
		}
	}
}

type topicObservableReconcileKind uint8

const (
	topicObservableNoop topicObservableReconcileKind = iota
	topicObservableRegister
	topicObservableUnregister
)

type topicObservableReconcileAction struct {
	kind          topicObservableReconcileKind
	generation    uint64
	registrations []func() error
}

func (c *topicObservableCollector) reconcileAction() topicObservableReconcileAction {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.registered {
		if len(c.sources) != 0 {
			return topicObservableReconcileAction{}
		}

		registrations := c.registrations
		c.registrations = nil
		c.registered = false
		c.activeGeneration = 0

		return topicObservableReconcileAction{
			kind:          topicObservableUnregister,
			registrations: registrations,
		}
	}
	if len(c.sources) == 0 {
		return topicObservableReconcileAction{}
	}

	c.nextGeneration++

	return topicObservableReconcileAction{
		kind:       topicObservableRegister,
		generation: c.nextGeneration,
	}
}

func (c *topicObservableCollector) commitRegistration(
	generation uint64,
	registrations []func() error,
) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.sources) == 0 {
		return false
	}

	c.registrations = registrations
	c.registered = true
	c.activeGeneration = generation

	return true
}

func (c *topicObservableCollector) removeSource(source *topicObservableSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sources, source)
}

func (c *topicObservableCollector) addSource(source *topicObservableSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sources[source] = struct{}{}
}

func (c *topicObservableCollector) registerTopicObservableMetrics(generation uint64) ([]func() error, bool) {
	registrations := make([]func() error, 0, len(c.metrics))
	for _, metric := range c.metrics {
		unregister, err := metric.vec.Register(c.callback(metric.kind, generation))
		if err != nil {
			unregisterTopicObservableMetrics(registrations)

			return nil, false
		}
		if unregister != nil {
			registrations = append(registrations, unregister)
		}
	}

	return registrations, true
}

func unregisterTopicObservableMetrics(registrations []func() error) {
	for i := len(registrations) - 1; i >= 0; i-- {
		if registrations[i] != nil {
			_ = registrations[i]()
		}
	}
}

func (c *topicObservableCollector) callback(
	kind topicObservableKind,
	generation uint64,
) ObservableGaugeCallback {
	return func(ctx context.Context, observe func(float64, map[string]string)) error {
		return c.observe(ctx, observe, kind, generation)
	}
}

func (c *topicObservableCollector) observe(
	ctx context.Context,
	observe func(float64, map[string]string),
	kind topicObservableKind,
	generation uint64,
) error {
	sources, ok := c.snapshotSources(generation)
	if !ok {
		return nil
	}

	values := make(map[topicObservableLabels]topicObservableValue, len(sources))
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return err
		}

		snapshot, labels, ok := source.snapshot(kind)
		if !ok {
			continue
		}

		value := observableSnapshotValue(snapshot, kind)
		key := topicObservableLabelsKey(labels)
		current, exists := values[key]
		if !exists {
			values[key] = topicObservableValue{value: value, labels: labels}

			continue
		}

		switch kind {
		case topicObservableSessionCount:
			current.value += value
		case topicObservableMessageAge, topicObservableCommitOffsetLag:
			if value > current.value {
				current.value = value
			}
		}
		values[key] = current
	}

	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		observe(value.value, value.labels)
	}

	return nil
}

func (s *topicObservableSource) snapshot(
	kind topicObservableKind,
) (trace.TopicReaderMetricsSnapshot, map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.active || (kind == topicObservableMessageAge && !s.age) ||
		(kind == topicObservableCommitOffsetLag && !s.lag) ||
		(kind == topicObservableSessionCount && !s.count) {
		return trace.TopicReaderMetricsSnapshot{}, nil, false
	}

	return s.source.Snapshot(), s.labels, true
}

func (s *topicObservableSource) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = false
	s.source = nil
	s.labels = nil
}

func (c *topicObservableCollector) snapshotSources(
	generation uint64,
) ([]*topicObservableSource, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeGeneration != generation {
		return nil, false
	}

	sources := make([]*topicObservableSource, 0, len(c.sources))
	for source := range c.sources {
		sources = append(sources, source)
	}

	return sources, true
}

func observableSnapshotValue(snapshot trace.TopicReaderMetricsSnapshot, kind topicObservableKind) float64 {
	switch kind {
	case topicObservableMessageAge:
		if snapshot.OldestMessageAge <= 0 {
			return 0
		}

		return snapshot.OldestMessageAge.Seconds()
	case topicObservableCommitOffsetLag:
		if snapshot.CommitOffsetLag <= 0 {
			return 0
		}

		return float64(snapshot.CommitOffsetLag)
	case topicObservableSessionCount:
		if snapshot.PartitionSessionCount <= 0 {
			return 0
		}

		return float64(snapshot.PartitionSessionCount)
	default:
		return 0
	}
}

func topicObservableLabelsKey(labels map[string]string) topicObservableLabels {
	return topicObservableLabels{
		endpoint:   labels["endpoint"],
		database:   labels["database"],
		consumer:   labels["consumer"],
		readerName: labels["reader.name"],
	}
}
