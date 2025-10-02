package log

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestLevelFromContext(t *testing.T) {
	for _, tt := range []struct {
		ctx context.Context //nolint:containedctx
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
		ctx   context.Context //nolint:containedctx
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

func TestWithNamesRaceRegression(t *testing.T) {
	count := 100
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx := WithNames(context.Background(), "test")
		ctx = WithNames(ctx, "test")
		ctx = WithNames(ctx, "test")
		res := make([]context.Context, count)

		start := make(chan bool)
		finished := make(chan bool)
		for i := 0; i < count; i++ {
			go func(index int) {
				<-start
				res[index] = WithNames(ctx, strconv.Itoa(index))
				finished <- true
			}(i)
		}

		time.Sleep(time.Microsecond)
		close(start)

		for i := 0; i < count; i++ {
			<-finished
		}

		for i := 0; i < count; i++ {
			expected := []string{"test", "test", "test", strconv.Itoa(i)}
			require.Equal(t, expected, NamesFromContext(res[i]))
		}
	})
}

func TestFieldsFromContext(t *testing.T) {
	for _, tt := range []struct {
		ctx    context.Context //nolint:containedctx
		fields []Field
	}{
		{
			ctx:    context.Background(),
			fields: nil,
		},
		{
			ctx:    WithFields(context.Background(), String("a", "1"), String("b", "1")),
			fields: []Field{String("a", "1"), String("b", "1")},
		},
		{
			ctx: WithFields(
				WithFields(context.Background(), String("a", "1"), String("b", "1")),
				String("a", "1"), String("b", "1"),
			),
			fields: []Field{String("a", "1"), String("b", "1"), String("a", "1"), String("b", "1")},
		},
		{
			ctx: WithFields(
				WithFields(context.Background(), String("a", "1"), String("b", "1")),
				String("a", "3"),
			),
			fields: []Field{String("a", "1"), String("b", "1"), String("a", "3")},
		},
	} {
		t.Run("", func(t *testing.T) {
			require.Equal(t, tt.fields, FieldsFromContext(tt.ctx))
		})
	}
}
