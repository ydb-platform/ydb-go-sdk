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
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v, %v), want (1, true, nil)", msg, ok, err)
	}
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v, %v), want (2, true, nil)", msg, ok, err)
	}
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 3 {
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
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg.Data != "a|b" {
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
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg.Data != "a" {
		t.Errorf("Receive() = (%v, %v, %v), want ({1, a}, true, nil)", msg, ok, err)
	}
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg.Data != "b" {
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
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v, %v), want (1, true, nil)", msg, ok, err)
	}
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v, %v), want (2, true, nil)", msg, ok, err)
	}

	// After buffer is empty, should return (0, false, nil)
	if msg, ok, err := ch.Receive(ctx); err != nil || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, nil)", msg, ok, err)
	}
}

func TestUnboundedChanReceiveAfterClose(t *testing.T) {
	ctx := context.Background()
	ch := NewUnboundedChan[int]()

	// Close empty channel
	ch.Close()

	// Should return (0, false, nil)
	if msg, ok, err := ch.Receive(ctx); err != nil || ok {
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
		if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != i {
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
		if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != i {
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
	if msg, ok, err := ch.Receive(ctx); !errors.Is(err, context.Canceled) || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, context.Canceled)", msg, ok, err)
	}
}

func TestUnboundedChanContextTimeout(t *testing.T) {
	ch := NewUnboundedChan[int]()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Should return context.DeadlineExceeded error after timeout
	start := time.Now()
	if msg, ok, err := ch.Receive(ctx); !errors.Is(err, context.DeadlineExceeded) || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, context.DeadlineExceeded)", msg, ok, err)
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

	// Start a goroutine that will send a message after a delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		ch.Send(42)
	}()

	// Start another goroutine that will cancel context after shorter delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Context cancellation should win
	if msg, ok, err := ch.Receive(ctx); !errors.Is(err, context.Canceled) || ok {
		t.Errorf("Receive() = (%v, %v, %v), want (0, false, context.Canceled)", msg, ok, err)
	}
}

func TestUnboundedChanMessageVsContext(t *testing.T) {
	ch := NewUnboundedChan[int]()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send message immediately
	ch.Send(42)

	// Start a goroutine that will cancel context after a delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Message should be received immediately
	if msg, ok, err := ch.Receive(ctx); err != nil || !ok || msg != 42 {
		t.Errorf("Receive() = (%v, %v, %v), want (42, true, nil)", msg, ok, err)
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
			for i := 0; i < count; i++ {
				ch.Send(i)
			}
			close(senderDone)
		}()

		// Start receiver goroutine
		go func() {
			received := make(map[int]bool)
			for {
				select {
				case <-senderDone:
					// After sender is done, check if we got all messages
					if len(received) == count {
						close(receiverDone)

						return
					}
					// If not all messages received, continue receiving
					if msg, ok, err := ch.Receive(ctx); err != nil {
						t.Errorf("Unexpected error: %v", err)
					} else if ok {
						if received[msg] {
							t.Errorf("Received duplicate message: %d", msg)
						}
						received[msg] = true
					}
				default:
					if msg, ok, err := ch.Receive(ctx); err != nil {
						t.Errorf("Unexpected error: %v", err)
					} else if ok {
						if received[msg] {
							t.Errorf("Received duplicate message: %d", msg)
						}
						received[msg] = true
					}
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
