package xsync

import (
	"testing"

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
	ch := NewUnboundedChan[int]()

	// Send some messages
	ch.Send(1)
	ch.Send(2)
	ch.Send(3)

	// Receive them in order
	if msg, ok := ch.Receive(); !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v), want (1, true)", msg, ok)
	}
	if msg, ok := ch.Receive(); !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v), want (2, true)", msg, ok)
	}
	if msg, ok := ch.Receive(); !ok || msg != 3 {
		t.Errorf("Receive() = (%v, %v), want (3, true)", msg, ok)
	}
}

func TestUnboundedChanSendWithMerge_ShouldMerge(t *testing.T) {
	ch := NewUnboundedChan[TestMessage]()

	// Send messages that should merge
	ch.SendWithMerge(TestMessage{ID: 1, Data: "a"}, mergeTestMessages)
	ch.SendWithMerge(TestMessage{ID: 1, Data: "b"}, mergeTestMessages)

	// Should get one merged message
	if msg, ok := ch.Receive(); !ok || msg.Data != "a|b" {
		t.Errorf("Receive() = (%v, %v), want ({1, a|b}, true)", msg, ok)
	}
}

func TestUnboundedChanSendWithMerge_ShouldNotMerge(t *testing.T) {
	ch := NewUnboundedChan[TestMessage]()

	// Send messages that should not merge
	ch.SendWithMerge(TestMessage{ID: 1, Data: "a"}, mergeTestMessages)
	ch.SendWithMerge(TestMessage{ID: 2, Data: "b"}, mergeTestMessages)

	// Should get both messages
	if msg, ok := ch.Receive(); !ok || msg.Data != "a" {
		t.Errorf("Receive() = (%v, %v), want ({1, a}, true)", msg, ok)
	}
	if msg, ok := ch.Receive(); !ok || msg.Data != "b" {
		t.Errorf("Receive() = (%v, %v), want ({2, b}, true)", msg, ok)
	}
}

func TestUnboundedChanClose(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Send some messages
	ch.Send(1)
	ch.Send(2)

	// Close the channel
	ch.Close()

	// Should still be able to receive buffered messages
	if msg, ok := ch.Receive(); !ok || msg != 1 {
		t.Errorf("Receive() = (%v, %v), want (1, true)", msg, ok)
	}
	if msg, ok := ch.Receive(); !ok || msg != 2 {
		t.Errorf("Receive() = (%v, %v), want (2, true)", msg, ok)
	}

	// After buffer is empty, should return (0, false)
	if msg, ok := ch.Receive(); ok {
		t.Errorf("Receive() = (%v, %v), want (0, false)", msg, ok)
	}
}

func TestUnboundedChanReceiveAfterClose(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Close empty channel
	ch.Close()

	// Should return (0, false)
	if msg, ok := ch.Receive(); ok {
		t.Errorf("Receive() = (%v, %v), want (0, false)", msg, ok)
	}
}

func TestUnboundedChanMultipleMessages(t *testing.T) {
	ch := NewUnboundedChan[int]()
	const count = 1000

	// Send many messages
	for i := 0; i < count; i++ {
		ch.Send(i)
	}

	// Receive them all
	for i := 0; i < count; i++ {
		if msg, ok := ch.Receive(); !ok || msg != i {
			t.Errorf("Receive() = (%v, %v), want (%d, true)", msg, ok, i)
		}
	}
}

func TestUnboundedChanSignalChannelBehavior(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Send multiple messages rapidly
	for i := 0; i < 100; i++ {
		ch.Send(i)
	}

	// Should receive all messages despite signal channel being buffered
	for i := 0; i < 100; i++ {
		if msg, ok := ch.Receive(); !ok || msg != i {
			t.Errorf("Receive() = (%v, %v), want (%d, true)", msg, ok, i)
		}
	}
}

func TestUnboundedChanConcurrentSendReceive(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
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
					if msg, ok := ch.Receive(); ok {
						if received[msg] {
							t.Errorf("Received duplicate message: %d", msg)
						}
						received[msg] = true
					}
				default:
					if msg, ok := ch.Receive(); ok {
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
		ch := NewUnboundedChan[TestMessage]()
		const count = 1000
		done := make(empty.Chan)

		// Start multiple sender goroutines
		for i := 0; i < 4; i++ {
			go func(id int) {
				for j := 0; j < count; j++ {
					ch.SendWithMerge(TestMessage{ID: id, Data: "msg"}, mergeTestMessages)
				}
				done <- empty.Struct{}
			}(i)
		}

		// Wait for all senders to finish
		for i := 0; i < 4; i++ {
			<-done
		}
		ch.Close()

		// Drain all messages and count 'msg' parts for each ID
		msgCounts := make(map[int]int)
		for {
			msg, ok := ch.Receive()
			if !ok {
				break
			}
			// Count number of 'msg' parts in msg.Data
			parts := 1
			for j := 0; j+3 < len(msg.Data); j++ {
				if msg.Data[j:j+4] == "|msg" {
					parts++
					j += 3
				}
			}
			msgCounts[msg.ID] += parts
		}

		// Check that for each ID, the total number of parts is count
		for i := 0; i < 4; i++ {
			if msgCounts[i] != count {
				t.Errorf("Total merged parts for ID %d = %d, want %d", i, msgCounts[i], count)
			}
		}
	})
}
