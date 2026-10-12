package main

import (
	"testing"
	"time"
)

func TestMakeMemoryReportUsesMeasurementDeltas(t *testing.T) {
	t.Parallel()

	report := makeMemoryReport(
		memorySnapshot{
			HeapAlloc:   100,
			HeapObjects: 10,
			TotalAlloc:  1000,
			Mallocs:     100,
			NumGC:       3,
		},
		memorySnapshot{
			HeapAlloc:   160,
			HeapObjects: 16,
			TotalAlloc:  1600,
			Mallocs:     140,
			NumGC:       5,
		},
		memorySnapshot{
			HeapAlloc:   120,
			HeapObjects: 12,
			TotalAlloc:  1700,
			Mallocs:     150,
			NumGC:       6,
		},
	)

	if report.AllocatedBytes != 600 {
		t.Fatalf("AllocatedBytes = %d, want 600", report.AllocatedBytes)
	}
	if report.Mallocs != 40 {
		t.Fatalf("Mallocs = %d, want 40", report.Mallocs)
	}
	if report.GCCycles != 2 {
		t.Fatalf("GCCycles = %d, want 2", report.GCCycles)
	}
	if report.HeapAfterGCBytes != 120 {
		t.Fatalf("HeapAfterGCBytes = %d, want 120", report.HeapAfterGCBytes)
	}
}

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
	if got, want := observation.Initial.ActivePartitionIDs, []int64{0}; !equalInt64s(got, want) {
		t.Fatalf("initial active partition IDs = %v, want %v", got, want)
	}
	if got, want := observation.Final.ActivePartitionIDs, []int64{1, 2, 3}; !equalInt64s(got, want) {
		t.Fatalf("final active partition IDs = %v, want %v", got, want)
	}
	if observation.Polls != 3 {
		t.Fatalf("Polls = %d, want 3", observation.Polls)
	}
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
