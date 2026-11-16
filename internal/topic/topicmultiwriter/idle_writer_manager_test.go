package topicmultiwriter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleWriterManager_AddAndGetRemovesWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	manager := newIdleWriterManager(ctx, time.Minute)

	writer := &writerWrapper{writer: &poolTestWriter{}}

	got, ok := manager.getWriterIfExists(1)
	require.False(t, ok)
	require.Nil(t, got)

	manager.addWriter(1, writer)

	got, ok = manager.getWriterIfExists(1)
	require.True(t, ok)
	require.Same(t, writer, got)

	got, ok = manager.getWriterIfExists(1)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestIdleWriterManager_ClosesIdleWriterAfterTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const idleTimeout = 10 * time.Millisecond

	manager := newIdleWriterManager(ctx, idleTimeout)

	testWriter := &poolTestWriter{}
	writer := &writerWrapper{writer: testWriter}
	manager.addWriter(1, writer)

	done := make(chan struct{})
	go func() {
		manager.run()
		close(done)
	}()

	require.Eventually(t, func() bool {
		removed := false
		manager.mu.WithLock(func() {
			_, exists := manager.idleWritersMap[1]
			removed = !exists
		})

		return removed && testWriter.closed.Load()
	}, time.Second, idleTimeout)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idleWriterManager.run did not stop after context cancel")
	}
}

func TestIdleWriterManager_CloseClosesAllWriters(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	manager := newIdleWriterManager(ctx, time.Minute)
	writer1 := &poolTestWriter{}
	writer2 := &poolTestWriter{}
	manager.addWriter(1, &writerWrapper{writer: writer1})
	manager.addWriter(2, &writerWrapper{writer: writer2})

	require.NoError(t, manager.close(ctx))
	require.True(t, writer1.closed.Load())
	require.True(t, writer2.closed.Load())
	require.Equal(t, 0, manager.getWritersCount())
}
