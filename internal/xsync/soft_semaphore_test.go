package xsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

//nolint:gocyclo
func TestSoftWeightedSemaphore(t *testing.T) {
	t.Run("NormalAcquireWithinCapacity", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)
		ctx := context.Background()

		// Should successfully acquire within capacity
		if err := sem.Acquire(ctx, 3); err != nil {
			t.Errorf("failed to acquire within capacity: %v", err)
		}

		// Should successfully release
		sem.Release(3)

		// Should successfully acquire again
		if err := sem.Acquire(ctx, 3); err != nil {
			t.Errorf("failed to acquire after release: %v", err)
		}
	})

	t.Run("OverflowAcquireWhenFree", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)
		ctx := context.Background()

		// Should successfully acquire more than capacity when free
		if err := sem.Acquire(ctx, 7); err != nil {
			t.Errorf("failed to acquire with overflow when free: %v", err)
		}

		// Next acquire should wait
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := sem.Acquire(ctx, 1); err != nil {
				t.Errorf("failed to acquire after overflow is released: %v", err)
			}
		}()

		// Check that second acquire is waiting
		select {
		case <-done:
			t.Error("acquire should wait when semaphore is overflown")
		case <-time.After(100 * time.Millisecond):
			// OK - acquire is waiting
		}

		// Release overflow
		sem.Release(7)

		// Check that second acquire succeeded
		select {
		case <-done:
			// OK - acquire succeeded after release
		case <-time.After(100 * time.Millisecond):
			t.Error("acquire should succeed after overflow is released")
		}
	})

	t.Run("TryAcquireOperations", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)

		// Should successfully acquire more than capacity when free
		if !sem.TryAcquire(7) {
			t.Error("TryAcquire should succeed with overflow when free")
		}

		// Next attempt should fail
		if sem.TryAcquire(1) {
			t.Error("TryAcquire should fail when semaphore is overflown")
		}

		// Release overflow
		sem.Release(7)

		// Now should successfully acquire
		if !sem.TryAcquire(3) {
			t.Error("TryAcquire should succeed after release")
		}
	})

	t.Run("PartialReleaseOfOverflow", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)
		ctx := context.Background()

		// Acquire with overflow
		if err := sem.Acquire(ctx, 7); err != nil {
			t.Errorf("failed to acquire with overflow: %v", err)
		}

		// Partially release (less than overflow)
		sem.Release(1)

		// Next acquire should still wait
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := sem.Acquire(ctx, 1); err != nil {
				t.Errorf("failed to acquire after partial release: %v", err)
			}
		}()

		// Check that acquire is waiting
		select {
		case <-done:
			t.Error("acquire should wait after partial release")
		case <-time.After(100 * time.Millisecond):
			// OK - acquire is waiting
		}

		// Release remaining
		sem.Release(6)

		// Check that acquire succeeded
		select {
		case <-done:
			// OK - acquire succeeded after full release
		case <-time.After(100 * time.Millisecond):
			t.Error("acquire should succeed after full release")
		}
	})

	t.Run("PartialReleaseAndAcquire", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)
		ctx := context.Background()

		// Acquire with overflow
		if err := sem.Acquire(ctx, 8); err != nil {
			t.Errorf("failed to acquire with overflow: %v", err)
		}

		// Release more than capacity but less than total weight
		sem.Release(6) // Release 6 out of 8, leaving 2 tokens held

		// Should be able to acquire up to 3 more tokens (capacity is 5, 2 are held)
		if err := sem.Acquire(ctx, 3); err != nil {
			t.Errorf("failed to acquire after partial release: %v", err)
		}

		// Should not be able to acquire more
		done := make(chan error)
		go func() {
			done <- sem.Acquire(ctx, 1)
		}()

		// Check that acquire is waiting
		select {
		case err := <-done:
			t.Errorf("acquire should wait, but got: %v", err)
		case <-time.After(100 * time.Millisecond):
			// OK - acquire is waiting
		}

		// Release all
		sem.Release(5) // Release remaining 5 tokens (2 + 3)

		// Now should succeed
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("acquire should succeed after full release, got: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("acquire should succeed after full release")
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		sem := NewSoftWeightedSemaphore(5)
		ctx, cancel := context.WithCancel(context.Background())

		// Acquire with overflow
		if err := sem.Acquire(ctx, 7); err != nil {
			t.Errorf("failed to acquire with overflow: %v", err)
		}

		// Try to acquire more
		done := make(chan error)
		go func() {
			defer close(done)
			done <- sem.Acquire(ctx, 1)
		}()

		// Cancel context
		cancel()

		// Check that we got cancellation error
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled, got %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("acquire should return immediately after context cancellation")
		}
	})

	t.Run("ConcurrentOperations", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			sem := NewSoftWeightedSemaphore(5)
			ctx := context.Background()
			const goroutines = 10
			done := make(chan struct{})

			// Launch multiple goroutines
			for i := 0; i < goroutines; i++ {
				go func() {
					defer func() {
						done <- struct{}{}
					}()

					// Each goroutine tries to acquire and release semaphore multiple times
					for j := 0; j < 10; j++ {
						if sem.TryAcquire(3) {
							time.Sleep(time.Millisecond)
							sem.Release(3)
						}

						if err := sem.Acquire(ctx, 2); err == nil {
							time.Sleep(time.Millisecond)
							sem.Release(2)
						}
					}
				}()
			}

			// Wait for all goroutines to complete
			for i := 0; i < goroutines; i++ {
				<-done
			}
		})
	})
}
