package topicreadercommon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamTraceACKBarrierDoesNotAllocate(t *testing.T) {
	for _, listener := range []bool{false, true} {
		_, q := StreamTrace(&trace.Topic{
			OnReaderCommittedNotify: func(trace.TopicReaderCommittedNotifyInfo) {},
		}, ReaderInfo{Listener: listener}, nil)
		stop := make(chan struct{})
		ok := true
		allocs := testing.AllocsPerRun(1000, func() {
			q.ackQueued.Store(true)
			ok = q.WaitForACKs(stop) && ok
		})
		q.Close()
		require.True(t, ok)
		require.Zero(t, allocs, "listener=%t", listener)
	}
}
