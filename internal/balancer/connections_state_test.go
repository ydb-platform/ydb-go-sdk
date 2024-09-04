package balancer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

func TestConnsToNodeIDMap(t *testing.T) {
	table := []struct {
		name   string
		source []conn.Conn
		res    map[uint32]conn.Conn
	}{
		{
			name:   "Empty",
			source: nil,
			res:    nil,
		},
		{
			name: "Zero",
			source: []conn.Conn{
				&mock.Conn{NodeIDField: 0},
			},
			res: map[uint32]conn.Conn{
				0: &mock.Conn{NodeIDField: 0},
			},
		},
		{
			name: "NonZero",
			source: []conn.Conn{
				&mock.Conn{NodeIDField: 1},
				&mock.Conn{NodeIDField: 10},
			},
			res: map[uint32]conn.Conn{
				1:  &mock.Conn{NodeIDField: 1},
				10: &mock.Conn{NodeIDField: 10},
			},
		},
		{
			name: "Combined",
			source: []conn.Conn{
				&mock.Conn{NodeIDField: 1},
				&mock.Conn{NodeIDField: 0},
				&mock.Conn{NodeIDField: 10},
			},
			res: map[uint32]conn.Conn{
				0:  &mock.Conn{NodeIDField: 0},
				1:  &mock.Conn{NodeIDField: 1},
				10: &mock.Conn{NodeIDField: 10},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.res, connsToNodeIDMap(test.source))
		})
	}
}

type filterFunc func(info balancerConfig.Info, e endpoint.Info) bool

func (f filterFunc) Allow(info balancerConfig.Info, e endpoint.Info) bool {
	return f(info, e)
}

func (f filterFunc) String() string {
	return "Custom"
}

func TestSortPreferConnections(t *testing.T) {
	table := []struct {
		name          string
		source        []conn.Conn
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Conn
		fallback      []conn.Conn
	}{
		{
			name:          "Empty",
			source:        nil,
			allowFallback: false,
			filter:        nil,
			prefer:        nil,
			fallback:      nil,
		},
		{
			name: "NilFilter",
			source: []conn.Conn{
				&mock.Conn{AddrField: "1"},
				&mock.Conn{AddrField: "2"},
			},
			allowFallback: false,
			filter:        nil,
			prefer: []conn.Conn{
				&mock.Conn{AddrField: "1"},
				&mock.Conn{AddrField: "2"},
			},
			fallback: nil,
		},
		{
			name: "FilterNoFallback",
			source: []conn.Conn{
				&mock.Conn{AddrField: "t1"},
				&mock.Conn{AddrField: "f1"},
				&mock.Conn{AddrField: "t2"},
				&mock.Conn{AddrField: "f2"},
			},
			allowFallback: false,
			filter: filterFunc(func(_ balancerConfig.Info, e endpoint.Info) bool {
				return strings.HasPrefix(e.Address(), "t")
			}),
			prefer: []conn.Conn{
				&mock.Conn{AddrField: "t1"},
				&mock.Conn{AddrField: "t2"},
			},
			fallback: nil,
		},
		{
			name: "FilterWithFallback",
			source: []conn.Conn{
				&mock.Conn{AddrField: "t1"},
				&mock.Conn{AddrField: "f1"},
				&mock.Conn{AddrField: "t2"},
				&mock.Conn{AddrField: "f2"},
			},
			allowFallback: true,
			filter: filterFunc(func(_ balancerConfig.Info, e endpoint.Info) bool {
				return strings.HasPrefix(e.Address(), "t")
			}),
			prefer: []conn.Conn{
				&mock.Conn{AddrField: "t1"},
				&mock.Conn{AddrField: "t2"},
			},
			fallback: []conn.Conn{
				&mock.Conn{AddrField: "f1"},
				&mock.Conn{AddrField: "f2"},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			prefer, fallback := sortPreferConnections(test.source, test.filter, balancerConfig.Info{}, test.allowFallback)
			require.Equal(t, test.prefer, prefer)
			require.Equal(t, test.fallback, fallback)
		})
	}
}

func TestSelectRandomConnection(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

	t.Run("Empty", func(t *testing.T) {
		c, failedCount := s.selectRandomConnection(nil, false)
		require.Nil(t, c)
		require.Equal(t, 0, failedCount)
	})

	t.Run("One", func(t *testing.T) {
		for _, goodState := range []conn.State{conn.Online, conn.Offline, conn.Created} {
			c, failedCount := s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: goodState}}, false)
			require.Equal(t, &mock.Conn{AddrField: "asd", State: goodState}, c)
			require.Equal(t, 0, failedCount)
		}
	})
	t.Run("OneBanned", func(t *testing.T) {
		c, failedCount := s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: conn.Banned}}, false)
		require.Nil(t, c)
		require.Equal(t, 1, failedCount)

		c, failedCount = s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: conn.Banned}}, true)
		require.Equal(t, &mock.Conn{AddrField: "asd", State: conn.Banned}, c)
		require.Equal(t, 0, failedCount)
	})
	t.Run("Two", func(t *testing.T) {
		conns := []conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online},
			&mock.Conn{AddrField: "2", State: conn.Online},
		}
		first := 0
		second := 0
		for i := 0; i < 100; i++ {
			c, _ := s.selectRandomConnection(conns, false)
			if c.Endpoint().Address() == "1" {
				first++
			} else {
				second++
			}
		}
		require.Equal(t, 100, first+second)
		require.InDelta(t, 50, first, 21)
		require.InDelta(t, 50, second, 21)
	})
	t.Run("TwoBanned", func(t *testing.T) {
		conns := []conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Banned},
			&mock.Conn{AddrField: "2", State: conn.Banned},
		}
		totalFailed := 0
		for i := 0; i < 100; i++ {
			c, failed := s.selectRandomConnection(conns, false)
			require.Nil(t, c)
			totalFailed += failed
		}
		require.Equal(t, 200, totalFailed)
	})
	t.Run("ThreeWithBanned", func(t *testing.T) {
		conns := []conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online},
			&mock.Conn{AddrField: "2", State: conn.Online},
			&mock.Conn{AddrField: "3", State: conn.Banned},
		}
		first := 0
		second := 0
		failed := 0
		for i := 0; i < 100; i++ {
			c, checkFailed := s.selectRandomConnection(conns, false)
			failed += checkFailed
			switch c.Endpoint().Address() {
			case "1":
				first++
			case "2":
				second++
			default:
				t.Error(c.Endpoint().Address())
			}
		}
		require.Equal(t, 100, first+second)
		require.InDelta(t, 50, first, 21)
		require.InDelta(t, 50, second, 21)
		require.Greater(t, 10, failed)
	})
}

func TestNewState(t *testing.T) {
	table := []struct {
		name  string
		state *connectionsState
		res   *connectionsState
	}{
		{
			name:  "Empty",
			state: newConnectionsState(nil, nil, balancerConfig.Info{}, false),
			res: &connectionsState{
				connByNodeID: nil,
				prefer:       nil,
				fallback:     nil,
				all:          nil,
			},
		},
		{
			name: "NoFilter",
			state: newConnectionsState([]conn.Conn{
				&mock.Conn{AddrField: "1", NodeIDField: 1},
				&mock.Conn{AddrField: "2", NodeIDField: 2},
			}, nil, balancerConfig.Info{}, false),
			res: &connectionsState{
				connByNodeID: map[uint32]conn.Conn{
					1: &mock.Conn{AddrField: "1", NodeIDField: 1},
					2: &mock.Conn{AddrField: "2", NodeIDField: 2},
				},
				prefer: []conn.Conn{
					&mock.Conn{AddrField: "1", NodeIDField: 1},
					&mock.Conn{AddrField: "2", NodeIDField: 2},
				},
				fallback: nil,
				all: []conn.Conn{
					&mock.Conn{AddrField: "1", NodeIDField: 1},
					&mock.Conn{AddrField: "2", NodeIDField: 2},
				},
			},
		},
		{
			name: "FilterDenyFallback",
			state: newConnectionsState([]conn.Conn{
				&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
				&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
				&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
			}, filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
				return info.SelfLocation == e.Location()
			}), balancerConfig.Info{SelfLocation: "t"}, false),
			res: &connectionsState{
				connByNodeID: map[uint32]conn.Conn{
					1: &mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					2: &mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					3: &mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
					4: &mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
				prefer: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				},
				fallback: nil,
				all: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				},
			},
		},
		{
			name: "FilterAllowFallback",
			state: newConnectionsState([]conn.Conn{
				&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
				&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
				&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
			}, filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
				return info.SelfLocation == e.Location()
			}), balancerConfig.Info{SelfLocation: "t"}, true),
			res: &connectionsState{
				connByNodeID: map[uint32]conn.Conn{
					1: &mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					2: &mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					3: &mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
					4: &mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
				prefer: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				},
				fallback: []conn.Conn{
					&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
				all: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
					&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
			},
		},
		{
			name: "WithNodeID",
			state: newConnectionsState([]conn.Conn{
				&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
				&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
				&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
			}, filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
				return info.SelfLocation == e.Location()
			}), balancerConfig.Info{SelfLocation: "t"}, true),
			res: &connectionsState{
				connByNodeID: map[uint32]conn.Conn{
					1: &mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					2: &mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					3: &mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
					4: &mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
				prefer: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
				},
				fallback: []conn.Conn{
					&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
				all: []conn.Conn{
					&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
					&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
					&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
					&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
				},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			require.NotNil(t, test.state.rand)
			test.state.rand = nil
			require.Equal(t, test.res, test.state)
		})
	}
}

func TestConnection(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConnection(context.Background())
		require.Nil(t, c)
		require.Equal(t, 0, failed)
	})
	t.Run("AllGood", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online},
			&mock.Conn{AddrField: "2", State: conn.Online},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConnection(context.Background())
		require.NotNil(t, c)
		require.Equal(t, 0, failed)
	})
	t.Run("WithBanned", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online},
			&mock.Conn{AddrField: "2", State: conn.Banned},
		}, nil, balancerConfig.Info{}, false)
		c, _ := s.GetConnection(context.Background())
		require.Equal(t, &mock.Conn{AddrField: "1", State: conn.Online}, c)
	})
	t.Run("AllBanned", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "t1", State: conn.Banned, LocationField: "t"},
			&mock.Conn{AddrField: "f2", State: conn.Banned, LocationField: "f"},
		}, filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
			return e.Location() == info.SelfLocation
		}), balancerConfig.Info{}, true)
		preferred := 0
		fallback := 0
		for i := 0; i < 100; i++ {
			c, failed := s.GetConnection(context.Background())
			require.NotNil(t, c)
			require.Equal(t, 2, failed)
			if c.Endpoint().Address() == "t1" {
				preferred++
			} else {
				fallback++
			}
		}
		require.Equal(t, 100, preferred+fallback)
		require.InDelta(t, 50, preferred, 21)
		require.InDelta(t, 50, fallback, 21)
	})
	t.Run("PreferBannedWithFallback", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "t1", State: conn.Banned, LocationField: "t"},
			&mock.Conn{AddrField: "f2", State: conn.Online, LocationField: "f"},
		}, filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
			return e.Location() == info.SelfLocation
		}), balancerConfig.Info{SelfLocation: "t"}, true)
		c, failed := s.GetConnection(context.Background())
		require.Equal(t, &mock.Conn{AddrField: "f2", State: conn.Online, LocationField: "f"}, c)
		require.Equal(t, 1, failed)
	})
	t.Run("PreferNodeID", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1},
			&mock.Conn{AddrField: "2", State: conn.Online, NodeIDField: 2},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConnection(endpoint.WithNodeID(context.Background(), 2))
		require.Equal(t, &mock.Conn{AddrField: "2", State: conn.Online, NodeIDField: 2}, c)
		require.Equal(t, 0, failed)
	})
	t.Run("PreferNodeIDWithBadState", func(t *testing.T) {
		s := newConnectionsState([]conn.Conn{
			&mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1},
			&mock.Conn{AddrField: "2", State: conn.Unknown, NodeIDField: 2},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConnection(endpoint.WithNodeID(context.Background(), 2))
		require.Equal(t, &mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1}, c)
		require.Equal(t, 0, failed)
	})
}
