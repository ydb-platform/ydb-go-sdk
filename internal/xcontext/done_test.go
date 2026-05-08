package xcontext

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithDone(t *testing.T) {
	t.Run("WithParentCancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		ctx1, _ := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel()
		require.Error(t, ctx1.Err())
	})
	t.Run("WithExplicitCancel", func(t *testing.T) {
		ctx := t.Context()
		done := make(chan struct{})
		ctx1, cancel1 := WithDone(ctx, done)
		require.NoError(t, ctx1.Err())
		cancel1()
		require.NoError(t, ctx.Err())
		require.Error(t, ctx1.Err())
	})
	t.Run("WithExplicitCloseDone", func(t *testing.T) {
		done := make(chan struct{})
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		close(done)
		<-ctx.Done()
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("WithClosedDone", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		ctx, cancel := WithDone(context.Background(), done)
		require.Error(t, ctx.Err())
		cancel()
	})
	t.Run("WithNilDone", func(t *testing.T) {
		var done chan struct{}
		ctx, cancel := WithDone(context.Background(), done)
		require.NoError(t, ctx.Err())
		cancel()
		require.Error(t, ctx.Err())
	})
}

// BenchmarkWithDone/AlreadyClosed-12         	 74847295	15.93 ns/op	   16 B/op	1 allocs/op
// BenchmarkWithDone/Open_CancelImmediately-12    4520635	263.6 ns/op	  256 B/op	4 allocs/op
// BenchmarkWithDone/Open_CloseDoneThenCancel-12  2363917	514.1 ns/op	  368 B/op	5 allocs/op
func BenchmarkWithDone(b *testing.B) {
	parent := context.Background()

	b.Run("AlreadyClosed", func(b *testing.B) {
		done := make(chan struct{})
		close(done)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			ctx, cancel := WithDone(parent, done)
			cancel()
			_ = ctx
		}
	})

	b.Run("Open_CancelImmediately", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			done := make(chan struct{})
			ctx, cancel := WithDone(parent, done)
			cancel()
			_ = ctx
			_ = done
		}
	})

	b.Run("Open_CloseDoneThenCancel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			done := make(chan struct{})
			ctx, cancel := WithDone(parent, done)
			close(done)
			<-ctx.Done()
			cancel()
		}
	})
}
