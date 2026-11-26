package topicreadercommon

import (
	"context"
	"runtime"
	"testing"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

func TestRetainedPartitionSessionDoesNotKeepActiveSessionsAfterShutdown(t *testing.T) {
	ctx := context.Background()
	kept := NewPartitionSession(ctx, "topic", 0, 1, "", 0, 0, 0)
	siblings := func() []weak.Pointer[PartitionSession] {
		var storage PartitionSessionStorage
		require.NoError(t, storage.Add(kept))
		pointers := make([]weak.Pointer[PartitionSession], 0, 100)
		for id := rawtopicreader.PartitionSessionID(1); id <= 100; id++ {
			session := NewPartitionSession(ctx, "topic", int64(id), 1, "", id, int64(id), 0)
			require.NoError(t, storage.Add(session))
			pointers = append(pointers, weak.Make(session))
		}

		storage.ReleaseActiveIndex()
		_, err := storage.Get(100)
		require.NoError(t, err, "commit responses still need protocol lookups")

		return pointers
	}()

	runtime.GC()
	runtime.GC()
	live := 0
	for _, pointer := range siblings {
		if pointer.Value() != nil {
			live++
		}
	}
	require.Zero(t, live, "siblings reachable from the retained session")
	runtime.KeepAlive(kept)
}

func TestPartitionSessionActiveIndexStaysReleasedAfterLateAssignment(t *testing.T) {
	ctx := context.Background()
	var storage PartitionSessionStorage
	storage.ReleaseActiveIndex()

	late := NewPartitionSession(ctx, "topic", 1, 1, "", 1, 1, 0)
	require.NoError(t, storage.Add(late))
	require.Empty(t, storage.ActiveCountsByTopic())
	require.Nil(t, late.active.Load(), "late assignments must not attach to the released index")
	_, err := storage.Remove(late.StreamPartitionSessionID)
	require.NoError(t, err)
	late.Close()
}
