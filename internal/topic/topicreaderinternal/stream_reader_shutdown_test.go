package topicreaderinternal

import (
	"runtime"
	"testing"
	"weak"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
)

func TestPullStreamRetainedPartitionSessionDoesNotKeepSiblingsAfterClose(t *testing.T) {
	var kept *topicreadercommon.PartitionSession
	siblings := make([]weak.Pointer[topicreadercommon.PartitionSession], 0, 100)
	if !t.Run("assign_and_close", func(t *testing.T) {
		env := newTopicReaderTestEnv(t)
		kept = env.partitionSession
		for id := partitionSessionID(100); id < 200; id++ {
			require.NoError(t, env.reader.onStartPartitionSessionRequest(&rawtopicreader.StartPartitionSessionRequest{
				PartitionSession: rawtopicreader.PartitionSession{PartitionSessionID: id, Path: "/test"},
			}))
			session, err := env.reader.sessionController.Get(id)
			require.NoError(t, err)
			siblings = append(siblings, weak.Make(session))
		}
		require.NoError(t, env.reader.CloseWithError(env.ctx, errReaderClosed))
	}) {
		return
	}

	runtime.GC()
	runtime.GC()
	live := 0
	for _, pointer := range siblings {
		if pointer.Value() != nil {
			live++
		}
	}
	require.Zero(t, live, "siblings reachable from the retained session after pull-stream shutdown")
	runtime.KeepAlive(kept)
}
