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

// BenchmarkWithDone/AlreadyClosed-12         	  67398998	17.58 ns/op	   16 B/op	1 allocs/op
// BenchmarkWithDone/Open_CancelImmediately-12     3005686	399.4 ns/op	  424 B/op	8 allocs/op
// BenchmarkWithDone/Open_CloseDoneThenCancel-12   1301546	922.2 ns/op	  648 B/op	10 allocs/op
func BenchmarkWithDone(b *testing.B) {
	b.Run("AlreadyClosed", func(b *testing.B) {
		done := make(chan struct{})
		close(done)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			ctx, cancel := WithDone(b.Context(), done)
			cancel()
			_ = ctx
		}
	})

	b.Run("Open_CancelImmediately", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			done := make(chan struct{})
			ctx, cancel := WithDone(b.Context(), done)
			cancel()
			_ = ctx
			_ = done
		}
	})

	b.Run("Open_CloseDoneThenCancel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			b.StopTimer()
			done := make(chan struct{})
			b.StartTimer()
			ctx, cancel := WithDone(b.Context(), done)
			close(done)
			<-ctx.Done()
			cancel()
		}
	})
}
