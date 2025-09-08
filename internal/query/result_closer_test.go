package query_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
)

func TestNewResultCloser(t *testing.T) {
	t.Run("empty closer should return nil if not closed", func(t *testing.T) {
		closer := query.NewResultCloser()

		assert.NoError(t, closer.Err())
	})
}

func TestResultCloser_Done(t *testing.T) {
	t.Run("empty closer Done() blocks", func(t *testing.T) {
		closer := query.NewResultCloser()

		select {
		case <-closer.Done():
			t.Fatal("done channel should not be closed")
		default:
		}
	})
}

func TestResultCloser_Close(t *testing.T) {
	someError := errors.New("some error")

	t.Run("closer should return io.EOF if closed with nil", func(t *testing.T) {
		closer := query.NewResultCloser()

		closer.Close(nil)

		assert.ErrorIs(t, closer.Err(), io.EOF)
	})

	t.Run("closer should return error if closed", func(t *testing.T) {
		closer := query.NewResultCloser()

		closer.Close(someError)

		assert.ErrorIs(t, closer.Err(), someError)
	})

	t.Run("closer should return first error if closed multiple times", func(t *testing.T) {
		closer := query.NewResultCloser()

		closer.Close(someError)
		closer.Close(io.EOF)

		assert.ErrorIs(t, closer.Err(), someError)
	})

	t.Run("closer Done() channel should be closed after Close()", func(t *testing.T) {
		closer := query.NewResultCloser()

		closer.Close(nil)

		select {
		case <-closer.Done():
		default:
			t.Fatal("done channel should be closed")
		}
	})
}

func TestResultCloser_CloseOnContextCancel(t *testing.T) {
	t.Run("closer should close on context cancel", func(t *testing.T) {
		closer := query.NewResultCloser()
		ctx, cancel := context.WithCancel(context.Background())

		closer.CloseOnContextCancel(ctx)
		cancel()

		select {
		case <-closer.Done():
			assert.ErrorIs(t, closer.Err(), context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("done channel should be closed")
		}
	})

	t.Run("closer should not close after CloseOnContextCancel() stop() method invoked", func(t *testing.T) {
		closer := query.NewResultCloser()
		ctx, cancel := context.WithCancel(context.Background())

		stop := closer.CloseOnContextCancel(ctx)
		stop()

		cancel()

		select {
		case <-closer.Done():
			t.Fatal("done channel should not be closed")
		case <-time.After(1 * time.Microsecond):
		}
	})
}

func TestResultCloser_OnClose(t *testing.T) {
	t.Run("closer should execute onClose once", func(t *testing.T) {
		closer := query.NewResultCloser()
		var (
			onCloseCalled1 atomic.Uint32
			onCloseCalled2 atomic.Uint32
		)

		closer.OnClose(func() {
			onCloseCalled1.Add(1)
		})

		closer.OnClose(func() {
			onCloseCalled2.Add(1)
		})

		wg := sync.WaitGroup{}
		wg.Add(2)

		go func() {
			closer.Close(nil)
			wg.Done()
		}()

		go func() {
			closer.Close(nil)
			wg.Done()
		}()
		wg.Wait()

		closer.Close(nil)

		assert.Equal(t, uint32(1), onCloseCalled1.Load())
		assert.Equal(t, uint32(1), onCloseCalled2.Load())
	})

	t.Run("deadlock when `onClose` execute `Close()`", func(t *testing.T) {
		closer := query.NewResultCloser()

		closer.OnClose(func() {
			closer.Close(nil)
		})

		closeEnded := make(chan struct{})

		go func() {
			closer.Close(nil)
			close(closeEnded)
		}()

		select {
		case <-closeEnded:
		case <-time.After(time.Second):
			t.Fatal("close should not deadlock")
		}
	})
}
