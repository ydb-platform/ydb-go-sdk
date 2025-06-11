package xsync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

type TestMessage struct {
	ID   int
	Data string
}

func mergeTestMessages(last, new TestMessage) (TestMessage, bool) {
	if last.ID == new.ID {
		return TestMessage{
			ID:   last.ID,
			Data: last.Data + "|" + new.Data,
		}, true
	}

	return new, false
}

func TestUnboundedChanBasicSendReceive(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()

	// Send some messages
	ch.Send(1)
	ch.Send(2)
	ch.Send(3)

	// Receive them in order
	msg, ok, err := ch.Receive(ctx)
	if err != nil || !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v, %v), want (1, true, nil)", msg, ok, err)
	}
	msg, ok, err = ch.Receive(ctx)
	if err != nil || !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v, %v), want (2, true, nil)", msg, ok, err)
	}
	msg, ok, err = ch.Receive(ctx)
	if err != nil || !ok || msg != 3 {
		t.Errorf("Receive() = (%v, %v, %v), want (3, true, nil)", msg, ok, err)
	}
}

func TestUnboundedChanSendWithMerge_ShouldMerge(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[TestMessage]()

	// Send messages that should merge
	ch.SendWithMerge(TestMessage{ID: 1, Data: "a"}, mergeTestMessages)
	ch.SendWithMerge(TestMessage{ID: 1, Data: "b"}, mergeTestMessages)

	// Should get one merged message
	msg, ok, err := ch.Receive(ctx)
	if err != nil || !ok || msg.Data != "a|b" {
		t.Errorf("Receive() = (%v, %v, %v), want ({1, a|b}, true, nil)", msg, ok, err)
	}
}

func TestUnboundedChanSendWithMerge_ShouldNotMerge(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[TestMessage]()

	// Send messages that should not merge
	ch.SendWithMerge(TestMessage{ID: 1, Data: "a"}, mergeTestMessages)
	ch.SendWithMerge(TestMessage{ID: 2, Data: "b"}, mergeTestMessages)

	// Should get both messages
	msg, ok, err := ch.Receive(ctx)
	if err != nil || !ok || msg.Data != "a" {
		t.Errorf("Receive() = (%v, %v, %v), want ({1, a}, true, nil)", msg, ok, err)
	}
	msg, ok, err = ch.Receive(ctx)
	if err != nil || !ok || msg.Data != "b" {
		t.Errorf("Receive() = (%v, %v, %v), want ({2, b}, true, nil)", msg, ok, err)
	}
}

func TestUnboundedChanClose(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()

	// Send some messages
	ch.Send(1)
	ch.Send(2)

	// Close the channel
	ch.Close()

	// Should still be able to receive buffered messages
	msg, ok, err := ch.Receive(ctx)
	if err != nil || !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v, %v), want (1, true, nil)", msg, ok, err)
	}
	msg, ok, err = ch.Receive(ctx)
	if err != nil || !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v, %v), want (2, true, nil)", msg, ok, err)
	}

	// After buffer is empty, should return (0, false, nil)
	msg, ok, err = ch.Receive(ctx)
	if err != nil || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, nil)", msg, ok, err)
	}
}

func TestUnboundedChanReceiveAfterClose(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()

	// Close empty channel
	ch.Close()

	// Should return (0, false, nil)
	msg, ok, err := ch.Receive(ctx)
	if err != nil || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, nil)", msg, ok, err)
	}
}

func TestUnboundedChanMultipleMessages(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()
	const count = 1000

	// Send many messages
	for i := 0; i < count; i++ {
		ch.Send(i)
	}

	// Receive them all
	for i := 0; i < count; i++ {
		msg, ok, err := ch.Receive(ctx)
		if err != nil || !ok || msg != i {
			t.Errorf("Receive() = (%v, %v, %v), want (%d, true, nil)", msg, ok, err, i)
		}
	}
}

func TestUnboundedChanSignalChannelBehavior(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()

	// Send multiple messages rapidly
	for i := 0; i < 100; i++ {
		ch.Send(i)
	}

	// Should receive all messages despite signal channel being buffered
	for i := 0; i < 100; i++ {
		msg, ok, err := ch.Receive(ctx)
		if err != nil || !ok || msg != i {
			t.Errorf("Receive() = (%v, %v, %v), want (%d, true, nil)", msg, ok, err, i)
		}
	}
}

// New context-specific tests
func TestUnboundedChanContextCancellation(t *testing.T) {
	ch := NewUnboundedChan[int]()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should return context.Canceled error
	_, ok, err := ch.Receive(ctx)
	if !errors.Is(err, context.Canceled) || ok {
		t.Errorf("Expected context.Canceled error and ok=false, got ok=%v, err=%v", ok, err)
	}
}

func TestUnboundedChanContextTimeout(t *testing.T) {
	ch := NewUnboundedChan[int]()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Should return context.DeadlineExceeded error after timeout
	start := time.Now()
	_, ok, err := ch.Receive(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || ok {
		t.Errorf("Expected context.DeadlineExceeded error and ok=false, got ok=%v, err=%v", ok, err)
	}
	elapsed := time.Since(start)
	if elapsed < 10*time.Millisecond {
		t.Errorf("Receive returned too quickly: %v", elapsed)
	}
}

func TestUnboundedChanContextVsMessage(t *testing.T) {
	ch := NewUnboundedChan[int]()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a goroutine that will send a message after context is cancelled
	go func() {
		xtest.WaitChannelClosed(t, ctx.Done())
		ch.Send(42)
	}()

	// Start another goroutine that will cancel context after shorter delay
	go func() {
		time.Sleep(2 * time.Millisecond)
		cancel()
	}()

	// Context cancellation should win
	_, ok, err := ch.Receive(ctx)
	if !errors.Is(err, context.Canceled) || ok {
		t.Errorf("Expected context.Canceled error and ok=false, got ok=%v, err=%v", ok, err)
	}
}

func TestUnboundedChanConcurrentSendReceive(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := context.Background()
		ch := NewUnboundedChan[int]()
		const count = 1000
		senderDone := make(empty.Chan)
		receiverDone := make(empty.Chan)

		// Start sender goroutine
		go func() {
			defer close(senderDone)
			for i := 0; i < count; i++ {
				ch.Send(i)
			}
		}()

		// Start receiver goroutine
		go func() {
			defer close(receiverDone)
			received := make(map[int]bool)
			receivedCount := 0
			maxReceiveAttempts := count + 100 // Allow some extra attempts
			attempts := 0

			for receivedCount < count && attempts < maxReceiveAttempts {
				attempts++
				msg, ok, err := ch.Receive(ctx)
				if err != nil {
					t.Errorf("Unexpected error: %v", err)

					return
				} else if ok {
					if received[msg] {
						t.Errorf("Received duplicate message: %d", msg)

						return
					}
					received[msg] = true
					receivedCount++
				} else {
					// Channel closed but we haven't received all messages
					break
				}
			}

			// Verify we received all expected messages
			if receivedCount != count {
				t.Errorf("Expected to receive %d messages, but received %d", count, receivedCount)
			}

			// Verify we received the correct messages
			for i := 0; i < count; i++ {
				if !received[i] {
					t.Errorf("Missing message: %d", i)

					break // Don't spam with too many errors
				}
			}
		}()

		// Wait for completion with timeout
		xtest.WaitChannelClosed(t, receiverDone)
	})
}

func TestUnboundedChanConcurrentMerge(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := context.Background()
		ch := NewUnboundedChan[TestMessage]()
		const count = 100 // Reduce count for faster test
		const numSenders = 4
		done := make(empty.Chan)

		// Start multiple sender goroutines
		for i := 0; i < numSenders; i++ {
			go func(id int) {
				for j := 0; j < count; j++ {
					ch.SendWithMerge(TestMessage{ID: id, Data: "test"}, mergeTestMessages)
				}
			}(i)
		}

		// Start receiver goroutine
		go func() {
			received := make(map[int]int)
			timeout := time.After(2 * time.Second)

			for {
				select {
				case <-timeout:
					close(done)

					return
				default:
					msg, ok, err := ch.Receive(ctx)
					if err != nil {
						t.Errorf("Unexpected error: %v", err)
						close(done)

						return
					}
					if ok {
						received[msg.ID]++
						// Check if we've received at least some messages from all senders
						if len(received) == numSenders && allSendersHaveMessages(received, numSenders) {
							close(done)

							return
						}
					}
				}
			}
		}()

		// Wait for completion
		xtest.WaitChannelClosed(t, done)
	})
}

// allSendersHaveMessages checks if all sender IDs have sent at least one message
func allSendersHaveMessages(received map[int]int, numSenders int) bool {
	for i := 0; i < numSenders; i++ {
		if received[i] == 0 {
			return false
		}
	}

	return true
}
