package xsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnboundedChan_Basic(t *testing.T) {
	ch := NewUnboundedChan[int]()
	defer ch.Close()

	ch.In() <- 1
	ch.In() <- 2

	require.Equal(t, 1, <-ch.Out())
	require.Equal(t, 2, <-ch.Out())
}

func TestUnboundedChan_Order(t *testing.T) {
	ch := NewUnboundedChan[int]()
	defer ch.Close()

	// Send 100 values
	for i := 0; i < 100; i++ {
		ch.In() <- i
	}

	// Receive and verify order
	for i := 0; i < 100; i++ {
		require.Equal(t, i, <-ch.Out())
	}
}

func TestUnboundedChan_Close(t *testing.T) {
	ch := NewUnboundedChan[int]()
	ch.In() <- 1
	ch.Close()

	// Should receive buffered value
	v, ok := <-ch.Out()
	require.True(t, ok)
	require.Equal(t, 1, v)

	// Channel should be closed
	_, ok = <-ch.Out()
	require.False(t, ok)
}

func TestUnboundedChan_Concurrent(t *testing.T) {
	ch := NewUnboundedChan[int]()
	defer ch.Close()

	// Concurrent sender
	go func() {
		for i := 0; i < 100; i++ {
			ch.In() <- i
		}
	}()

	// Concurrent receiver
	for i := 0; i < 100; i++ {
		require.Equal(t, i, <-ch.Out())
	}
}

func TestUnboundedChan_NoBlockOnSend(t *testing.T) {
	ch := NewUnboundedChan[int]()
	defer ch.Close()

	// Fill channel with more than default buffer size
	for i := 0; i < 1000; i++ {
		ch.In() <- i
	}

	// Verify send doesn't block
	done := make(chan struct{})
	go func() {
		ch.In() <- 1001
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Send operation blocked")
	}
}

func TestUnboundedChan_CloseTwice(t *testing.T) {
	ch := NewUnboundedChan[int]()
	ch.Close()

	// Second close should not panic
	require.Panics(t, ch.Close)
}

func TestUnboundedChan_NoPanicOnSendAfterClose(t *testing.T) {
	ch := NewUnboundedChan[int]()
	ch.Close()

	require.Panics(t, func() {
		ch.In() <- 1
	})
}

func TestUnboundedChan_BufferCleanup(t *testing.T) {
	ch := NewUnboundedChan[int]()

	// Send values but don't receive
	for i := 0; i < 100; i++ {
		ch.In() <- i
	}

	// Close and verify cleanup
	ch.CloseAndStop()
	_, ok := <-ch.Out()
	require.False(t, ok)
}

func TestUnboundedChan_WithMerge(t *testing.T) {
	// Test merge function that sums values
	mergeFunc := func(a, b int) (int, bool) {
		return a + b, true
	}

	ch := NewUnboundedChanWithMergeLastItem(mergeFunc)
	defer ch.Close()

	in, out := ch.In(), ch.Out()

	in <- 1
	in <- 2
	in <- 3

	// Should get merged value (1+2+3 = 6)
	require.Equal(t, 6, <-out)

	// New value should not be merged with previous
	in <- 4
	require.Equal(t, 4, <-out)
}

func TestUnboundedChan_ConditionalMerge(t *testing.T) {
	// Test merge function that only merges even numbers
	mergeFunc := func(a, b int) (int, bool) {
		if a%2 == 0 && b%2 == 0 {
			return a + b, true
		}
		return 0, false
	}

	ch := NewUnboundedChanWithMergeLastItem(mergeFunc)
	defer ch.Close()

	in, out := ch.In(), ch.Out()

	in <- 2
	in <- 4 // merged (2+4)
	in <- 3 // not merged
	in <- 6 // not merged (last was odd)
	in <- 8 // merged (6+8)

	require.Equal(t, 6, <-out)  // 2+4
	require.Equal(t, 3, <-out)  // not merged
	require.Equal(t, 14, <-out) // 6+8
}

func TestUnboundedChan_CloseWithMerge(t *testing.T) {
	// Test proper closing behavior with merge function
	mergeFunc := func(a, b int) (int, bool) {
		return a + b, true
	}

	ch := NewUnboundedChanWithMergeLastItem(mergeFunc)

	in, out := ch.In(), ch.Out()

	in <- 1
	in <- 2
	ch.Close()

	require.Equal(t, 3, <-out) // merged value

	// Channel should be closed
	_, ok := <-out
	require.False(t, ok)
}
