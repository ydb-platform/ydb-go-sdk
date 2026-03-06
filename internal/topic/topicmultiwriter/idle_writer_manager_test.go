package topicmultiwriter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdleWriterManager_AddAndGetRemovesWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
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

func TestIdleWriterManager_RemovesIdleWriterAfterTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const idleTimeout = 10 * time.Millisecond

	manager := newIdleWriterManager(ctx, idleTimeout)

	writer := &writerWrapper{writer: &poolTestWriter{}}
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

		return removed
	}, time.Second, idleTimeout)

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idleWriterManager.run did not stop after context cancel")
	}
}
