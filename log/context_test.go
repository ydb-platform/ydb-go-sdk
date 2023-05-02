package log

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLevelFromContext(t *testing.T) {
	for _, tt := range []struct {
		ctx context.Context
		lvl Level
	}{
		{
			ctx: context.Background(),
			lvl: TRACE,
		},
		{
			ctx: WithLevel(context.Background(), INFO),
			lvl: INFO,
		},
		{
			ctx: WithLevel(WithLevel(context.Background(), ERROR), INFO),
			lvl: INFO,
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.lvl, LevelFromContext(tt.ctx))
		})
	}
}

func TestNamesFromContext(t *testing.T) {
	for _, tt := range []struct {
		ctx   context.Context
		names []string
	}{
		{
			ctx:   context.Background(),
			names: []string{},
		},
		{
			ctx:   WithNames(context.Background(), "a", "b"),
			names: []string{"a", "b"},
		},
		{
			ctx:   WithNames(WithNames(context.Background(), "a", "b"), "c", "d"),
			names: []string{"a", "b", "c", "d"},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.names, NamesFromContext(tt.ctx))
		})
	}
}
