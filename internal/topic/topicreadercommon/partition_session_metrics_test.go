package topicreadercommon

import (
	"context"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

func TestRetainedPartitionSessionDoesNotKeepRemovedSessions(t *testing.T) {
	// A retained message keeps its session alive after the stream is gone.
	kept := NewPartitionSession(context.Background(), "topic", 0, 1, "", 0, 0, 0)
	removed := func() []weak.Pointer[PartitionSession] {
		storage := &PartitionSessionStorage{}
		require.NoError(t, storage.Add(kept))
		pointers := make([]weak.Pointer[PartitionSession], 0, 100)
		for id := rawtopicreader.PartitionSessionID(1); id <= 100; id++ {
			session := NewPartitionSession(context.Background(), "topic", int64(id), 1, "", id, int64(id), 0)
			require.NoError(t, storage.Add(session))
			_, err := storage.Remove(id)
			require.NoError(t, err)
			pointers = append(pointers, weak.Make(session))
		}

		return pointers
	}()
	runtime.GC()
	runtime.GC()
	for i, pointer := range removed {
		require.Nil(t, pointer.Value(), "removed session %d is still reachable from a retained session", i+1)
	}
	runtime.KeepAlive(kept)
}

func TestPartitionSessionActiveCounts(t *testing.T) {
	storage := &PartitionSessionStorage{}
	first := NewPartitionSession(context.Background(), "topic", 1, 1, "", 1, 1, 0)
	second := NewPartitionSession(context.Background(), "topic", 2, 1, "", 2, 2, 0)
	t.Cleanup(first.Close)
	t.Cleanup(second.Close)
	require.NoError(t, storage.Add(first))
	require.NoError(t, storage.Add(second))
	require.Equal(t, map[string]int64{"topic": 2}, storage.ActiveCountsByTopic())
	first.SetNoMoreMessages()
	require.Equal(t, int64(2), storage.ActiveCountsByTopic()["topic"])
	first.Close()
	require.Equal(t, int64(1), storage.ActiveCountsByTopic()["topic"])
	_, err := storage.Remove(second.StreamPartitionSessionID)
	require.NoError(t, err)
	require.Empty(t, storage.ActiveCountsByTopic())
	require.Len(t, storage.GetAll(), 2)
}

func TestPartitionSessionCountSource(t *testing.T) {
	ctx := context.Background()
	var callback telemetry.Int64GaugeCallback
	meter := func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
		callback = cb

		return func() error { return nil }, nil
	}
	storage := &PartitionSessionStorage{}
	selectors := []*PublicReadSelector{{Path: "topic"}, {Path: "/Root/db/topic"}, {Path: "empty"}}
	_, err := RegisterPartitionSessionCount(
		ReaderMetricsConfig{Meter: meter, Database: "//Root/db/"},
		"consumer", selectors, func() (map[string]int64, error) {
			return storage.ActiveCountsByTopic(), nil
		},
	)
	require.NoError(t, err)
	session := NewPartitionSession(ctx, "/Root/db/topic", 1, 1, "", 1, 1, 0)
	t.Cleanup(session.Close)
	require.NoError(t, storage.Add(session))
	values := make(map[string]int64)
	require.NoError(t, callback(ctx, func(value int64, attributes ...telemetry.Attribute) {
		require.Contains(t, attributes, telemetry.Attribute{Key: "reader.name", Value: "default"})
		var topic string
		for _, attr := range attributes {
			if attr.Key == "topic" {
				topic = attr.Value
			}
		}
		require.NotContains(t, values, topic)
		values[topic] = value
	}))
	require.Equal(t, map[string]int64{"/Root/db/topic": 1, "/Root/db/empty": 0}, values)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, callback(canceled, func(int64, ...telemetry.Attribute) {
		t.Error("canceled collection produced an observation")
	}), context.Canceled)
}

func TestPartitionSessionCountDuringContextReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	storage := &PartitionSessionStorage{}
	session := NewPartitionSession(ctx, "topic", 1, 1, "", 1, 1, 0)
	require.NoError(t, storage.Add(session))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			session.SetContext(ctx)
		}
	}()
	for range 1000 {
		require.Equal(t, int64(1), storage.ActiveCountsByTopic()["topic"])
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	session.Close()
	require.Empty(t, storage.ActiveCountsByTopic())
}

func TestPartitionSessionCountWithoutConsumer(t *testing.T) {
	var callback telemetry.Int64GaugeCallback
	_, err := RegisterPartitionSessionCount(
		ReaderMetricsConfig{Meter: func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
			callback = cb

			return func() error { return nil }, nil
		}},
		"", []*PublicReadSelector{{Path: "topic"}}, func() (map[string]int64, error) {
			return map[string]int64{}, nil
		},
	)
	require.NoError(t, err)
	var observations [][]telemetry.Attribute
	require.NoError(t, callback(context.Background(), func(_ int64, attrs ...telemetry.Attribute) {
		observations = append(observations, attrs)
	}))
	require.Len(t, observations, 1)
	for _, attr := range observations[0] {
		require.NotEqual(t, "consumer", attr.Key)
	}
}

func TestPartitionSessionCountConcurrentWithFirstLookup(t *testing.T) {
	storage := &PartitionSessionStorage{}
	lookupDone := make(chan error, 1)
	go func() {
		_, err := storage.Get(1)
		lookupDone <- err
	}()
	require.Empty(t, storage.ActiveCountsByTopic())
	require.Error(t, <-lookupDone)
}

func TestPartitionSessionCountStopsOnCancellation(t *testing.T) {
	var callback telemetry.Int64GaugeCallback
	_, err := RegisterPartitionSessionCount(
		ReaderMetricsConfig{Meter: func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
			callback = cb

			return func() error { return nil }, nil
		}},
		"consumer", []*PublicReadSelector{{Path: "first"}, {Path: "second"}, {Path: "third"}},
		func() (map[string]int64, error) { return map[string]int64{}, nil },
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observations := 0
	err = callback(ctx, func(int64, ...telemetry.Attribute) {
		observations++
		cancel()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, observations)
}

func BenchmarkPartitionSessionActiveCountsWithRetainedSessions(b *testing.B) {
	const retained = 100000
	storage := &PartitionSessionStorage{}
	for id := rawtopicreader.PartitionSessionID(1); id <= retained; id++ {
		session := NewPartitionSession(context.Background(), "topic", int64(id), 1, "", id, int64(id), 0)
		require.NoError(b, storage.Add(session))
		_, err := storage.Remove(id)
		require.NoError(b, err)
		session.Close()
	}
	active := NewPartitionSession(context.Background(), "topic", 0, 1, "", retained+1, retained+1, 0)
	b.Cleanup(active.Close)
	require.NoError(b, storage.Add(active))
	require.Equal(b, map[string]int64{"topic": 1}, storage.ActiveCountsByTopic())

	b.ResetTimer()
	for range b.N {
		storage.ActiveCountsByTopic()
	}
}

func TestPartitionSessionCountObservationsAreIndependent(t *testing.T) {
	var callback telemetry.Int64GaugeCallback
	_, err := RegisterPartitionSessionCount(
		ReaderMetricsConfig{Meter: func(_ telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
			callback = cb

			return func() error { return nil }, nil
		}},
		"consumer", []*PublicReadSelector{{Path: "first"}, {Path: "second"}}, func() (map[string]int64, error) {
			return map[string]int64{}, nil
		},
	)
	require.NoError(t, err)
	var observations [][]telemetry.Attribute
	require.NoError(t, callback(context.Background(), func(_ int64, attrs ...telemetry.Attribute) {
		observations = append(observations, attrs)
	}))
	require.Len(t, observations, 2)
	require.NotEqual(t, observations[0], observations[1])
}
