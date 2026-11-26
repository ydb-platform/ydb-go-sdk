package topicreadercommon

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

func TestActiveCountsIgnoreRepeatedPartitionSessionTransitions(t *testing.T) {
	var storage PartitionSessionStorage
	graceful := NewPartitionSession(t.Context(), "topic", 1, 1, "", 1, 1, 0)
	forced := NewPartitionSession(t.Context(), "topic", 2, 1, "", 2, 2, 0)
	other := NewPartitionSession(t.Context(), "other", 3, 1, "", 3, 3, 0)
	closed := NewPartitionSession(t.Context(), "topic", 4, 1, "", 4, 4, 0)
	t.Cleanup(other.Close)
	t.Cleanup(graceful.Close)
	t.Cleanup(forced.Close)
	closed.Close()

	require.NoError(t, storage.Add(graceful))
	require.NoError(t, storage.Add(forced))
	require.NoError(t, storage.Add(other))
	require.NoError(t, storage.Add(closed))
	require.Error(t, storage.Add(graceful))
	want := map[string]int64{"topic": 2, "other": 1}
	require.Equal(t, want, storage.ActiveCountsByTopic())

	graceful.SetNoMoreMessages()
	require.Equal(t, want, storage.ActiveCountsByTopic(), "graceful stop does not retire until local close")
	graceful.Close()
	graceful.Close()
	_, err := storage.Remove(graceful.StreamPartitionSessionID)
	require.NoError(t, err)
	require.Equal(t, map[string]int64{"topic": 1, "other": 1}, storage.ActiveCountsByTopic())

	for range 2 {
		_, err = storage.Remove(forced.StreamPartitionSessionID)
		require.NoError(t, err)
	}
	forced.Close()
	require.Equal(t, map[string]int64{"other": 1}, storage.ActiveCountsByTopic())

	storage.ReleaseActiveIndex()
	other.Close()
	storage.ReleaseActiveIndex()
	require.Empty(t, storage.ActiveCountsByTopic())
	require.Len(t, storage.GetAll(), 4, "release leaves commit lookups intact")
}

func BenchmarkActiveCountsByTopic(b *testing.B) {
	for _, partitions := range []int{1, 100000} {
		b.Run(strconv.Itoa(partitions), func(b *testing.B) {
			var storage PartitionSessionStorage
			for i := range partitions {
				id := rawtopicreader.PartitionSessionID(i)
				session := NewPartitionSession(b.Context(), "topic", int64(i), 1, "", id, 0, 0)
				require.NoError(b, storage.Add(session))
			}
			b.ReportAllocs()
			for b.Loop() {
				if got := storage.ActiveCountsByTopic()["topic"]; got != int64(partitions) {
					b.Fatalf("got %d, want %d", got, partitions)
				}
			}
		})
	}
}
