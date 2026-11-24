package topicreadercommon

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

func BenchmarkCommitMessageTrackerIncrementalAck(b *testing.B) {
	for _, n := range []rawtopiccommon.Offset{10000, 30000} {
		b.Run(fmt.Sprintf("ranges=%d", n), func(b *testing.B) {
			for range b.N {
				tracker := NewCommitMessageTracker(0)
				for i := range n {
					tracker.Queue(i, i+1)
				}
				for i := range n {
					tracker.Acknowledge(i + 1)
				}
			}
		})
	}
}
