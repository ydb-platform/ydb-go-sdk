package params

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestParameter(t *testing.T) {
	p := Named("x", value.TextValue("X"))
	require.Equal(t, "x", p.Name())
	require.EqualValues(t, "X", p.Value())
	require.Equal(t, "DECLARE x AS Utf8", Declare(p))
}

func TestParameters(t *testing.T) {
	p := &Parameters{}
	p.Add(
		Named("x", value.TextValue("X")),
		Named("y", value.TextValue("Y")),
	)
	require.Equal(t, "{\"x\":\"X\"u,\"y\":\"Y\"u}", p.String())
	require.Equal(t, 2, p.Count())
	visited := make(map[string]value.Value, 2)
	p.Each(func(name string, v value.Value) {
		visited[name] = v
	})
	require.Len(t, visited, 2)
	require.EqualValues(t, map[string]value.Value{
		"x": value.TextValue("X"),
		"y": value.TextValue("Y"),
	}, visited)
}

func TestNil(t *testing.T) {
	for _, tt := range []struct {
		name string
		p    *Parameters
	}{
		{
			name: xtest.CurrentFileLine(),
			p:    nil,
		},
		{
			name: xtest.CurrentFileLine(),
			p:    &Parameters{},
		},
		{
			name: xtest.CurrentFileLine(),
			p:    Builder{}.Build(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, "{}", tt.p.String())
			require.Equal(t, 0, tt.p.Count())
			visited := make(map[string]value.Value, 1)
			tt.p.Each(func(name string, v value.Value) {
				visited[name] = v
			})
			require.Empty(t, visited)
			a := allocator.New()
			defer a.Free()
			require.Empty(t, tt.p.ToYDB(a))
		})
	}
}
