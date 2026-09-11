package metrics

import (
	"context"
	"errors"
	"maps"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestTopicObservableMetricsUnsupportedAndDetails(t *testing.T) {
	legacy := topic(recordingConfig{
		registry: newRecordingRegistry(),
		details:  trace.DetailsAll,
	})
	require.Nil(t, legacy.OnReaderMetricsSource)

	config := newObservableConfig(trace.TopicReaderCustomerEvents)
	tracer := topic(config)

	require.Nil(t, tracer.OnReaderMetricsSource)
	require.Empty(t, config.observables.names())
}

func TestTopicObservableMetricsDescriptorsFollowDetails(t *testing.T) {
	config := newObservableConfig(trace.TopicReaderMessageEvents)
	tracer := topic(config)

	require.NotNil(t, tracer.OnReaderMetricsSource)
	require.Equal(t, []string{topicReaderMessageAgeName}, config.observables.names())
	require.Equal(t, []string{"s"}, config.observables.units())

	config = newObservableConfig(trace.TopicReaderStreamEvents)
	topic(config)
	require.ElementsMatch(t, []string{
		topicReaderCommitOffsetLagName,
		topicReaderSessionCountName,
	}, config.observables.names())
}

func TestTopicObservableMetricsAggregateAndRecreate(t *testing.T) {
	config := newObservableConfig(trace.DetailsAll)
	tracer := topic(config)
	labels := map[string]string{
		"endpoint":    "node",
		"database":    "/db",
		"consumer":    "consumer",
		"reader.name": "reader",
	}

	first := &testTopicMetricsSource{snapshot: trace.TopicReaderMetricsSnapshot{
		OldestMessageAge:      2 * time.Second,
		CommitOffsetLag:       7,
		PartitionSessionCount: 2,
	}}
	second := &testTopicMetricsSource{snapshot: trace.TopicReaderMetricsSnapshot{
		OldestMessageAge:      5 * time.Second,
		CommitOffsetLag:       3,
		PartitionSessionCount: 4,
	}}

	doneFirst := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, first))
	doneSecond := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, second))
	require.NotNil(t, doneFirst)
	require.NotNil(t, doneSecond)

	observations := config.observables.collect(t, topicReaderMessageAgeName)
	require.Len(t, observations, 1)
	require.Equal(t, labels, observations[0].labels)
	require.Equal(t, float64(5), observations[0].value)

	observations = config.observables.collect(t, topicReaderCommitOffsetLagName)
	require.Len(t, observations, 1)
	require.Equal(t, float64(7), observations[0].value)

	observations = config.observables.collect(t, topicReaderSessionCountName)
	require.Len(t, observations, 1)
	require.Equal(t, float64(6), observations[0].value)

	emptyLabels := map[string]string{
		"endpoint":    "node-2",
		"database":    "/db",
		"consumer":    "consumer",
		"reader.name": "reader-2",
	}
	doneEmpty := tracer.OnReaderMetricsSource(startTopicMetricsSource(
		emptyLabels,
		&testTopicMetricsSource{},
	))
	observations = config.observables.collect(t, topicReaderMessageAgeName)
	require.Len(t, observations, 2)
	emptyObservation, ok := observationFor(observations, emptyLabels)
	require.True(t, ok)
	require.Zero(t, emptyObservation.value)

	doneFirst(trace.TopicReaderMetricsSourceDoneInfo{})
	doneFirst(trace.TopicReaderMetricsSourceDoneInfo{})
	doneSecond(trace.TopicReaderMetricsSourceDoneInfo{})
	doneEmpty(trace.TopicReaderMetricsSourceDoneInfo{})
	for _, name := range config.observables.names() {
		require.Zero(t, config.observables.callbackCount(name))
	}

	doneNew := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, first))
	require.NotNil(t, doneNew)
	for _, name := range config.observables.names() {
		require.Equal(t, 1, config.observables.callbackCount(name))
	}
	doneNew(trace.TopicReaderMetricsSourceDoneInfo{})
}

func TestTopicObservableMetricsRegistrationFailureIsBestEffort(t *testing.T) {
	config := newObservableConfig(trace.DetailsAll)
	config.observables.setFailure(topicReaderCommitOffsetLagName)
	tracer := topic(config)
	labels := map[string]string{
		"endpoint":    "node",
		"database":    "/db",
		"consumer":    "consumer",
		"reader.name": "reader",
	}

	doneFirst := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, &testTopicMetricsSource{}))
	require.NotNil(t, doneFirst)
	require.Zero(t, config.observables.callbackCount(topicReaderMessageAgeName))

	config.observables.setFailure("")
	doneSecond := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, &testTopicMetricsSource{
		snapshot: trace.TopicReaderMetricsSnapshot{CommitOffsetLag: 3},
	}))
	require.NotNil(t, doneSecond)
	require.Equal(t, 1, config.observables.callbackCount(topicReaderMessageAgeName))
	observations := config.observables.collect(t, topicReaderCommitOffsetLagName)
	require.Len(t, observations, 1)
	require.Equal(t, float64(3), observations[0].value)

	doneFirst(trace.TopicReaderMetricsSourceDoneInfo{})
	doneSecond(trace.TopicReaderMetricsSourceDoneInfo{})
}

func TestTopicObservableMetricsStaleCallbackAfterUnregisterFailure(t *testing.T) {
	config := newObservableConfig(trace.DetailsAll)
	tracer := topic(config)
	labels := map[string]string{
		"endpoint":    "node",
		"database":    "/db",
		"consumer":    "consumer",
		"reader.name": "reader",
	}

	old := &testTopicMetricsSource{snapshot: trace.TopicReaderMetricsSnapshot{
		OldestMessageAge: time.Second,
	}}
	doneOld := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, old))
	config.observables.setUnregisterFailure(topicReaderMessageAgeName)
	doneOld(trace.TopicReaderMetricsSourceDoneInfo{})
	require.Equal(t, 1, config.observables.callbackCount(topicReaderMessageAgeName))

	config.observables.setUnregisterFailure("")
	newSource := &testTopicMetricsSource{snapshot: trace.TopicReaderMetricsSnapshot{
		OldestMessageAge: 7 * time.Second,
	}}
	doneNew := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, newSource))
	require.Equal(t, 2, config.observables.callbackCount(topicReaderMessageAgeName))

	observations := config.observables.collect(t, topicReaderMessageAgeName)
	require.Len(t, observations, 1)
	require.Equal(t, float64(7), observations[0].value)

	doneNew(trace.TopicReaderMetricsSourceDoneInfo{})
	require.Equal(t, 1, config.observables.callbackCount(topicReaderMessageAgeName))
	require.Empty(t, config.observables.collect(t, topicReaderMessageAgeName))
}

func TestTopicObservableMetricsContextCancellation(t *testing.T) {
	config := newObservableConfig(trace.DetailsAll)
	tracer := topic(config)
	done := tracer.OnReaderMetricsSource(startTopicMetricsSource(
		map[string]string{"reader.name": "reader"},
		&testTopicMetricsSource{snapshot: trace.TopicReaderMetricsSnapshot{
			OldestMessageAge: time.Second,
		}},
	))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	observations, err := config.observables.collectContext(ctx, topicReaderMessageAgeName)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, observations)

	done(trace.TopicReaderMetricsSourceDoneInfo{})
}

func TestTopicObservableMetricsListenerDetails(t *testing.T) {
	config := newObservableConfig(trace.TopicListenerStreamEvents)
	tracer := topic(config)
	labels := map[string]string{
		"endpoint":    "node",
		"database":    "/db",
		"consumer":    "consumer",
		"reader.name": "reader",
	}

	readerDone := tracer.OnReaderMetricsSource(startTopicMetricsSource(labels, &testTopicMetricsSource{
		snapshot: trace.TopicReaderMetricsSnapshot{OldestMessageAge: time.Second},
	}))
	require.Zero(t, config.observables.callbackCount(topicReaderMessageAgeName))
	readerDone(trace.TopicReaderMetricsSourceDoneInfo{})

	listenerInfo := startTopicMetricsSource(labels, &testTopicMetricsSource{
		snapshot: trace.TopicReaderMetricsSnapshot{
			OldestMessageAge:      2 * time.Second,
			CommitOffsetLag:       3,
			PartitionSessionCount: 4,
		},
	})
	listenerInfo.Listener = true
	listenerDone := tracer.OnReaderMetricsSource(listenerInfo)
	require.Equal(t, 1, config.observables.callbackCount(topicReaderMessageAgeName))
	require.Equal(t, 1, config.observables.callbackCount(topicReaderCommitOffsetLagName))
	require.Equal(t, 1, config.observables.callbackCount(topicReaderSessionCountName))
	require.Equal(t, float64(2), config.observables.collect(t, topicReaderMessageAgeName)[0].value)

	listenerDone(trace.TopicReaderMetricsSourceDoneInfo{})
}

func TestTopicObservableMetricsConcurrentLifecycle(t *testing.T) {
	config := newObservableConfig(trace.DetailsAll)
	tracer := topic(config)
	const sourceCount = 16

	dones := make(chan func(trace.TopicReaderMetricsSourceDoneInfo), sourceCount)
	var start sync.WaitGroup
	for i := range sourceCount {
		start.Add(1)
		go func(i int) {
			defer start.Done()
			name := "reader-" + string(rune('a'+i))
			dones <- tracer.OnReaderMetricsSource(startTopicMetricsSource(
				map[string]string{
					"endpoint":    "node",
					"database":    "/db",
					"consumer":    "consumer",
					"reader.name": name,
				},
				&testTopicMetricsSource{},
			))
		}(i)
	}
	start.Wait()
	close(dones)

	var collect sync.WaitGroup
	collect.Add(1)
	go func() {
		defer collect.Done()
		for range 32 {
			_, _ = config.observables.collectErr(topicReaderMessageAgeName)
		}
	}()

	for done := range dones {
		done(trace.TopicReaderMetricsSourceDoneInfo{})
	}
	collect.Wait()
	require.Zero(t, config.observables.callbackCount(topicReaderMessageAgeName))
}

func TestTopicObservableMetricsComposeEmptyHookIsNil(t *testing.T) {
	composed := gtrace.Compose(&trace.Topic{}, &trace.Topic{})
	require.Nil(t, composed.OnReaderMetricsSource)
}

func startTopicMetricsSource(
	labels map[string]string,
	source trace.TopicReaderMetricsSource,
) trace.TopicReaderMetricsSourceStartInfo {
	return trace.TopicReaderMetricsSourceStartInfo{
		Endpoint:   labels["endpoint"],
		Database:   labels["database"],
		Consumer:   labels["consumer"],
		ReaderName: labels["reader.name"],
		Source:     source,
	}
}

func observationFor(
	observations []recordingObservableObservation,
	labels map[string]string,
) (recordingObservableObservation, bool) {
	for _, observation := range observations {
		if observation.labels["reader.name"] == labels["reader.name"] {
			return observation, true
		}
	}

	return recordingObservableObservation{}, false
}

type testTopicMetricsSource struct {
	snapshot trace.TopicReaderMetricsSnapshot
}

func (s *testTopicMetricsSource) Snapshot() trace.TopicReaderMetricsSnapshot {
	return s.snapshot
}

type observableConfig struct {
	recordingConfig

	observables *recordingObservableRegistry
}

func newObservableConfig(details trace.Details) observableConfig {
	return observableConfig{
		recordingConfig: recordingConfig{
			registry: newRecordingRegistry(),
			details:  details,
		},
		observables: newRecordingObservableRegistry(),
	}
}

func (c observableConfig) WithSystem(system string) Config {
	c.recordingConfig = c.recordingConfig.WithSystem(system).(recordingConfig)

	return c
}

func (c observableConfig) ObservableGaugeVecWithDescriptor(
	name, unit string,
	labelNames ...string,
) ObservableGaugeVec {
	return c.observables.vector(name, unit, labelNames)
}

type recordingObservableRegistry struct {
	mu                sync.Mutex
	vectors           map[string]*recordingObservableVec
	failure           string
	unregisterFailure string
}

func newRecordingObservableRegistry() *recordingObservableRegistry {
	return &recordingObservableRegistry{vectors: make(map[string]*recordingObservableVec)}
}

func (r *recordingObservableRegistry) vector(
	name, unit string,
	labelNames []string,
) ObservableGaugeVec {
	r.mu.Lock()
	defer r.mu.Unlock()

	if vector, ok := r.vectors[name]; ok {
		return vector
	}
	vector := &recordingObservableVec{
		registry:   r,
		name:       name,
		unit:       unit,
		labelNames: append([]string(nil), labelNames...),
		callbacks:  make(map[int]ObservableGaugeCallback),
	}
	r.vectors[name] = vector

	return vector
}

func (r *recordingObservableRegistry) setFailure(name string) {
	r.mu.Lock()
	r.failure = name
	r.mu.Unlock()
}

func (r *recordingObservableRegistry) setUnregisterFailure(name string) {
	r.mu.Lock()
	r.unregisterFailure = name
	r.mu.Unlock()
}

func (r *recordingObservableRegistry) names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	names := make([]string, 0, len(r.vectors))
	for name := range r.vectors {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func (r *recordingObservableRegistry) units() []string {
	names := r.names()
	units := make([]string, 0, len(names))
	for _, name := range names {
		r.mu.Lock()
		units = append(units, r.vectors[name].unit)
		r.mu.Unlock()
	}

	return units
}

func (r *recordingObservableRegistry) callbackCount(name string) int {
	r.mu.Lock()
	vector := r.vectors[name]
	r.mu.Unlock()
	if vector == nil {
		return 0
	}

	return vector.callbackCount()
}

func (r *recordingObservableRegistry) collect(t *testing.T, name string) []recordingObservableObservation {
	t.Helper()
	observations, err := r.collectErr(name)
	require.NoError(t, err)

	return observations
}

func (r *recordingObservableRegistry) collectErr(name string) ([]recordingObservableObservation, error) {
	return r.collectContext(context.Background(), name)
}

func (r *recordingObservableRegistry) collectContext(
	ctx context.Context,
	name string,
) ([]recordingObservableObservation, error) {
	r.mu.Lock()
	vector := r.vectors[name]
	r.mu.Unlock()
	if vector == nil {
		return nil, nil
	}

	return vector.collect(ctx)
}

type recordingObservableVec struct {
	registry   *recordingObservableRegistry
	name       string
	unit       string
	labelNames []string

	mu        sync.Mutex
	next      int
	callbacks map[int]ObservableGaugeCallback
}

func (v *recordingObservableVec) Register(callback ObservableGaugeCallback) (func() error, error) {
	v.registry.mu.Lock()
	failure := v.registry.failure == v.name
	v.registry.mu.Unlock()
	if failure {
		return nil, errors.New("registration failed")
	}

	v.mu.Lock()
	id := v.next
	v.next++
	v.callbacks[id] = callback
	v.mu.Unlock()

	var once sync.Once
	var unregisterErr error

	return func() error {
		once.Do(func() {
			v.registry.mu.Lock()
			failure := v.registry.unregisterFailure == v.name
			v.registry.mu.Unlock()
			if failure {
				unregisterErr = errors.New("unregistration failed")

				return
			}

			v.mu.Lock()
			delete(v.callbacks, id)
			v.mu.Unlock()
		})

		return unregisterErr
	}, nil
}

func (v *recordingObservableVec) callbackCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()

	return len(v.callbacks)
}

func (v *recordingObservableVec) collect(ctx context.Context) ([]recordingObservableObservation, error) {
	v.mu.Lock()
	callbacks := make([]ObservableGaugeCallback, 0, len(v.callbacks))
	for _, callback := range v.callbacks {
		callbacks = append(callbacks, callback)
	}
	v.mu.Unlock()

	observations := make([]recordingObservableObservation, 0)
	for _, callback := range callbacks {
		err := callback(ctx, func(value float64, labels map[string]string) {
			copied := make(map[string]string, len(labels))
			maps.Copy(copied, labels)
			observations = append(observations, recordingObservableObservation{
				value:  value,
				labels: copied,
			})
		})
		if err != nil {
			return observations, err
		}
	}

	return observations, nil
}

type recordingObservableObservation struct {
	value  float64
	labels map[string]string
}
