package transactionalwriterbenchmark

import (
	"slices"
	"testing"
	"time"
)

func TestTopologyRecorderObservesFirstActivePartitionSplit(t *testing.T) {
	t.Parallel()

	startedAt := time.Unix(100, 0)
	recorder := newTopologyRecorder(startedAt, topicTopology{
		ActivePartitionIDs: []int64{0},
		TotalPartitions:    1,
	})
	recorder.record(startedAt.Add(time.Second), topicTopology{
		ActivePartitionIDs: []int64{0},
		TotalPartitions:    1,
	})
	recorder.record(startedAt.Add(3*time.Second), topicTopology{
		ActivePartitionIDs: []int64{1, 2},
		TotalPartitions:    3,
	})
	recorder.record(startedAt.Add(4*time.Second), topicTopology{
		ActivePartitionIDs: []int64{1, 2, 3},
		TotalPartitions:    4,
	})

	observation := recorder.snapshot()
	if !observation.SplitObserved {
		t.Fatal("SplitObserved = false, want true")
	}
	if observation.FirstSplitAfter != 3*time.Second {
		t.Fatalf("FirstSplitAfter = %s, want 3s", observation.FirstSplitAfter)
	}
	if got, want := observation.Initial.ActivePartitionIDs, []int64{0}; !slices.Equal(got, want) {
		t.Fatalf("initial active partition IDs = %v, want %v", got, want)
	}
	if got, want := observation.Final.ActivePartitionIDs, []int64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("final active partition IDs = %v, want %v", got, want)
	}
	if observation.Polls != 3 {
		t.Fatalf("Polls = %d, want 3", observation.Polls)
	}
}
