package topicreadercommon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/telemetry"
)

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
	meter := func(desc telemetry.Descriptor, cb telemetry.Int64GaugeCallback) (func() error, error) {
		require.Equal(t, "ydb.topic.reader.partition_session.count", desc.Name)
		require.Equal(t, "{session}", desc.Unit)
		callback = cb

		return func() error {
			callback = nil

			return nil
		}, nil
	}
	storage := &PartitionSessionStorage{}
	selectors := []*PublicReadSelector{{Path: "topic"}, {Path: "/Root/db/topic"}, {Path: "empty"}}
	first, err := RegisterPartitionSessionCount(
		ReaderMetricsConfig{Meter: meter, Endpoint: "localhost:2135", Database: "//Root/db/"},
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
		attrs := make(map[string]string)
		for _, attr := range attributes {
			attrs[attr.Key] = attr.Value
		}
		require.Equal(t, map[string]string{
			"endpoint": "localhost:2135", "database": "/Root/db", "consumer": "consumer",
			"reader.name": "default", "topic": attrs["topic"],
		}, attrs)
		values[attrs["topic"]] = value
	}))
	require.Equal(t, map[string]int64{"/Root/db/topic": 1, "/Root/db/empty": 0}, values)
	require.NoError(t, first())
	require.Nil(t, callback)
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
