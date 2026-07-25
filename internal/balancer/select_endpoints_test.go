package balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

func discoveredEndpoints(n int) []endpoint.Endpoint {
	out := make([]endpoint.Endpoint, n)
	for i := range out {
		out[i] = endpoint.New(
			fmt.Sprintf("e%d.example:2135", i),
			endpoint.WithID(uint32(i+1)),
		)
	}

	return out
}

func TestSelectEndpoints(t *testing.T) {
	t.Parallel()

	discovered := discoveredEndpoints(20)

	t.Run("Unlimited", func(t *testing.T) {
		selected := selectEndpoints(nil, discovered, 0, xrand.New(xrand.WithLock()))
		require.Equal(t, discovered, selected)
	})

	t.Run("BelowCap", func(t *testing.T) {
		small := discovered[:3]
		selected := selectEndpoints(nil, small, 10, xrand.New(xrand.WithLock()))
		require.Equal(t, small, selected)
	})

	t.Run("Cap", func(t *testing.T) {
		selected := selectEndpoints(nil, discovered, 5, xrand.New(xrand.WithLock()))
		require.Len(t, selected, 5)
	})

	t.Run("StickyKeep", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Online},
			&mock.Conn{AddrField: "e1.example:2135", NodeIDField: 2, StateField: state.Online},
			&mock.Conn{AddrField: "e2.example:2135", NodeIDField: 3, StateField: state.Online},
		}
		selected := selectEndpoints(previous, discovered, 3, xrand.New(xrand.WithLock()))
		require.Len(t, selected, 3)
		require.Equal(t, "e0.example:2135", selected[0].Address())
		require.Equal(t, "e1.example:2135", selected[1].Address())
		require.Equal(t, "e2.example:2135", selected[2].Address())
	})

	t.Run("SkipBannedInSticky", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Banned},
			&mock.Conn{AddrField: "e1.example:2135", NodeIDField: 2, StateField: state.Online},
		}
		selected := selectEndpoints(previous, discovered, 2, xrand.New(xrand.WithLock()))
		require.Len(t, selected, 2)
		require.Equal(t, "e1.example:2135", selected[0].Address())
		require.NotEqual(t, "e0.example:2135", selected[1].Address())
	})

	t.Run("FillAfterDrop", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Online},
			&mock.Conn{AddrField: "gone.example:2135", NodeIDField: 99, StateField: state.Online},
		}
		selected := selectEndpoints(previous, discovered, 2, xrand.New(xrand.WithLock()))
		require.Len(t, selected, 2)
		require.Equal(t, "e0.example:2135", selected[0].Address())
		require.NotEqual(t, "gone.example:2135", selected[1].Address())
	})
}
