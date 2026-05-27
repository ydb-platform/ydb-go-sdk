package stats_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
)

// recorder collects callback invocations in the order they happen, so tests
// can assert both "was called" and "in what order relative to other callbacks".
type recorder struct {
	calls []string
}

func (r *recorder) record(name string) func(stats.QueryStats) {
	return func(stats.QueryStats) {
		r.calls = append(r.calls, name)
	}
}

func TestWithModeCallback(t *testing.T) {
	t.Run("NilCallbackReturnsSameContext", func(t *testing.T) {
		parent := context.Background()

		ctx := stats.WithModeCallback(parent, stats.ModeBasic, nil)

		require.Equal(t, parent, ctx)
		require.Nil(t, stats.ModeCallbackFromContext(ctx))
	})

	t.Run("NilCallbackKeepsExistingValue", func(t *testing.T) {
		var r recorder
		parent := stats.WithModeCallback(context.Background(), stats.ModeFull, r.record("existing"))

		ctx := stats.WithModeCallback(parent, stats.ModeBasic, nil)

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeFull, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"existing"}, r.calls)
	})

	t.Run("StoresFirstCallback", func(t *testing.T) {
		var r recorder

		ctx := stats.WithModeCallback(context.Background(), stats.ModeBasic, r.record("first"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeBasic, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"first"}, r.calls)
	})

	t.Run("ChainsCallbacksInOrder", func(t *testing.T) {
		// Regression: previously WithModeCallback overwrote the context value,
		// so a second WithStatsMode* call silently dropped the first callback.
		var r recorder

		ctx := context.Background()
		ctx = stats.WithModeCallback(ctx, stats.ModeBasic, r.record("first"))
		ctx = stats.WithModeCallback(ctx, stats.ModeBasic, r.record("second"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		got.Callback(nil)
		require.Equal(t, []string{"first", "second"}, r.calls)
	})

	t.Run("UpgradesModeWhenChainedWithMoreDetailed", func(t *testing.T) {
		var r recorder

		ctx := context.Background()
		ctx = stats.WithModeCallback(ctx, stats.ModeBasic, r.record("basic"))
		ctx = stats.WithModeCallback(ctx, stats.ModeProfile, r.record("profile"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeProfile, got.Mode, "max(basic, profile) must win so detail is preserved")
		got.Callback(nil)
		require.Equal(t, []string{"basic", "profile"}, r.calls)
	})

	t.Run("KeepsHigherModeWhenChainedWithLessDetailed", func(t *testing.T) {
		var r recorder

		ctx := context.Background()
		ctx = stats.WithModeCallback(ctx, stats.ModeProfile, r.record("profile"))
		ctx = stats.WithModeCallback(ctx, stats.ModeBasic, r.record("basic"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeProfile, got.Mode, "already-requested detail must not be downgraded")
		got.Callback(nil)
		require.Equal(t, []string{"profile", "basic"}, r.calls)
	})

	t.Run("KeepsModeWhenChainedAtSameDetail", func(t *testing.T) {
		var r recorder

		ctx := context.Background()
		ctx = stats.WithModeCallback(ctx, stats.ModeFull, r.record("first"))
		ctx = stats.WithModeCallback(ctx, stats.ModeFull, r.record("second"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeFull, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"first", "second"}, r.calls)
	})

	t.Run("ChainsThreeCallbacks", func(t *testing.T) {
		var r recorder

		ctx := context.Background()
		ctx = stats.WithModeCallback(ctx, stats.ModeBasic, r.record("a"))
		ctx = stats.WithModeCallback(ctx, stats.ModeFull, r.record("b"))
		ctx = stats.WithModeCallback(ctx, stats.ModeProfile, r.record("c"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeProfile, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"a", "b", "c"}, r.calls)
	})

	t.Run("AllModes", func(t *testing.T) {
		for _, mode := range []stats.Mode{stats.ModeBasic, stats.ModeFull, stats.ModeProfile} {
			t.Run(modeName(mode), func(t *testing.T) {
				ctx := stats.WithModeCallback(context.Background(), mode, func(stats.QueryStats) {})

				got := stats.ModeCallbackFromContext(ctx)
				require.NotNil(t, got)
				require.Equal(t, mode, got.Mode)
			})
		}
	})
}

func TestModeCallbackFromContext(t *testing.T) {
	t.Run("ReturnsNilForEmptyContext", func(t *testing.T) {
		require.Nil(t, stats.ModeCallbackFromContext(context.Background()))
	})

	t.Run("ReturnsStoredValue", func(t *testing.T) {
		var r recorder
		ctx := stats.WithModeCallback(context.Background(), stats.ModeFull, r.record("cb"))

		got := stats.ModeCallbackFromContext(ctx)

		require.NotNil(t, got)
		require.Equal(t, stats.ModeFull, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"cb"}, r.calls)
	})
}

func TestModeCallbackFromContextWith(t *testing.T) {
	t.Run("ReturnsProvidedCallbackWhenContextEmpty", func(t *testing.T) {
		var r recorder

		got := stats.ModeCallbackFromContextWith(context.Background(), stats.ModeBasic, r.record("internal"))

		require.NotNil(t, got)
		require.Equal(t, stats.ModeBasic, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"internal"}, r.calls)
	})

	t.Run("CombinesContextAndProvidedCallback", func(t *testing.T) {
		var r recorder
		ctx := stats.WithModeCallback(context.Background(), stats.ModeFull, r.record("user"))

		got := stats.ModeCallbackFromContextWith(ctx, stats.ModeBasic, r.record("internal"))

		require.NotNil(t, got)
		require.Equal(t, stats.ModeFull, got.Mode, "max(user mode, internal mode) wins")
		got.Callback(nil)
		require.Equal(t, []string{"user", "internal"}, r.calls, "ctx callback runs before the provided one")
	})

	t.Run("UpgradesModeWhenProvidedIsHigher", func(t *testing.T) {
		var r recorder
		ctx := stats.WithModeCallback(context.Background(), stats.ModeBasic, r.record("user"))

		got := stats.ModeCallbackFromContextWith(ctx, stats.ModeProfile, r.record("internal"))

		require.Equal(t, stats.ModeProfile, got.Mode)
	})

	t.Run("DoesNotMutateContext", func(t *testing.T) {
		// The combined callback is meant to be ephemeral; subsequent reads of
		// the same ctx must still see only what the user put there.
		var r recorder
		ctx := stats.WithModeCallback(context.Background(), stats.ModeBasic, r.record("user"))

		_ = stats.ModeCallbackFromContextWith(ctx, stats.ModeFull, r.record("internal"))

		got := stats.ModeCallbackFromContext(ctx)
		require.NotNil(t, got)
		require.Equal(t, stats.ModeBasic, got.Mode)
		got.Callback(nil)
		require.Equal(t, []string{"user"}, r.calls, "only the ctx callback should fire on the un-combined value")
	})
}

func modeName(m stats.Mode) string {
	switch m {
	case stats.ModeBasic:
		return "Basic"
	case stats.ModeFull:
		return "Full"
	case stats.ModeProfile:
		return "Profile"
	default:
		return "Unknown"
	}
}
