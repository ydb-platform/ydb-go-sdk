package topicproducer

import (
	"context"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

// testWriter is a minimal implementation of writer used to observe Close calls.
type testWriter struct {
	closed bool
}

func (w *testWriter) Close(_ context.Context) error {
	w.closed = true

	return nil
}

func (w *testWriter) WaitInit(_ context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, nil
}

func (w *testWriter) Write(_ context.Context, _ []topicwriterinternal.PublicMessage) error {
	return nil
}

func TestIdleWritersSupervisor_AddAndRemoveUpdatesIndex(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &worker{ctx: ctx}
	s := newIdleWritersSupervisor(ctx, w, time.Minute)

	if s.idleWriters.Len() != 0 {
		t.Fatalf("expected no idle writers initially, got %d", s.idleWriters.Len())
	}

	// Add two partitions.
	s.add(1)
	s.add(2)

	if s.idleWriters.Len() != 2 {
		t.Fatalf("expected 2 idle writers, got %d", s.idleWriters.Len())
	}

	if _, ok := s.idleWritersIndex[1]; !ok {
		t.Fatalf("expected partition 1 to be in index")
	}
	if _, ok := s.idleWritersIndex[2]; !ok {
		t.Fatalf("expected partition 2 to be in index")
	}

	// Remove first partition.
	s.remove(1)
	if _, ok := s.idleWritersIndex[1]; ok {
		t.Fatalf("expected partition 1 to be removed from index")
	}
	if s.idleWriters.Len() != 1 {
		t.Fatalf("expected 1 idle writer after removal, got %d", s.idleWriters.Len())
	}

	// Remove second partition.
	s.remove(2)
	if len(s.idleWritersIndex) != 0 {
		t.Fatalf("expected index to be empty, got %d entries", len(s.idleWritersIndex))
	}
	if s.idleWriters.Len() != 0 {
		t.Fatalf("expected no idle writers after all removals, got %d", s.idleWriters.Len())
	}

	// Removing non-existent partition should not panic.
	s.remove(42)
}

func TestIdleWritersSupervisor_RemovesIdleWriterAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &worker{
		ctx:     ctx,
		writers: make(map[int64]*writerWrapper),
	}

	const partitionID int64 = 1
	tw := &testWriter{}
	w.writers[partitionID] = &writerWrapper{writer: tw}

	// Use small timeout to make the test fast.
	supervisor := newIdleWritersSupervisor(ctx, w, 10*time.Millisecond)

	// Register the writer as idle.
	supervisor.add(partitionID)

	done := make(chan struct{})
	go func() {
		supervisor.run()
		close(done)
	}()

	// Wait long enough for timeout to expire and supervisor to act.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	if !tw.closed {
		t.Fatalf("expected writer to be closed by idleWritersSupervisor")
	}
	if _, ok := w.writers[partitionID]; ok {
		t.Fatalf("expected writer to be removed from worker.writers")
	}
}
