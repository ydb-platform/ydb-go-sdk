package topicreadercommon

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

func liveHeapAfterGC() int64 {
	var stats runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&stats)

	return int64(stats.HeapAlloc)
}

// The protocol history keeps every session, so the storage of the active index
// is measured as the heap that releasing the index frees.
func TestActiveTopicCountsReleasePeakStorage(t *testing.T) {
	const (
		topics = 100_000
		limit  = 256 << 10
	)
	for _, survivors := range []int{0, 10} {
		t.Run(strconv.Itoa(survivors)+"_left", func(t *testing.T) {
			storage := &PartitionSessionStorage{}
			storage.TrackActive()
			want := make(map[string]int64)
			sessions := make([]*PartitionSession, topics)
			for i := range sessions {
				id := rawtopicreader.PartitionSessionID(i)
				sessions[i] = NewPartitionSession(t.Context(), "topic-"+strconv.Itoa(i), int64(i), 1, "", id, int64(i), 0)
				require.NoError(t, storage.Add(sessions[i]))
			}
			// The peak needs every topic active at once.
			for i, session := range sessions {
				if i < survivors {
					want[session.Topic] = 1

					continue
				}
				_, err := storage.Remove(session.StreamPartitionSessionID)
				require.NoError(t, err)
				session.Close()
			}
			clear(sessions)
			// A scrape is the only place allowed to walk a sparse map.
			require.Equal(t, want, storage.ActiveCountsByTopic())

			late := NewPartitionSession(t.Context(), "late", topics, 1, "", topics, topics, 0)
			require.NoError(t, storage.Add(late))
			want["late"] = 1
			require.Equal(t, want, storage.ActiveCountsByTopic())

			withIndex := liveHeapAfterGC()
			storage.ReleaseActiveIndex()
			retained := withIndex - liveHeapAfterGC()
			require.Less(t, retained, int64(limit))
			runtime.KeepAlive(storage)
		})
	}
}
