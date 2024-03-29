package balancer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

func TestConnsToNodeIDMap(t *testing.T) {
	table := []struct {
		name   string
		source []conn.Info
		res    map[uint32]conn.Info
	}{
		{
			name:   "Empty",
			source: nil,
			res:    nil,
		},
		{
			name: "Zero",
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 0}},
			},
			res: map[uint32]conn.Info{
				0: &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 0}},
			},
		},
		{
			name: "NonZero",
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 1}},
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 10}},
			},
			res: map[uint32]conn.Info{
				1:  &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 1}},
				10: &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 10}},
			},
		},
		{
			name: "Combined",
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 1}},
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 0}},
				&mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 10}},
			},
			res: map[uint32]conn.Info{
				0:  &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 0}},
				1:  &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 1}},
				10: &mock.Conn{EndpointField: &mock.Endpoint{NodeIDField: 10}},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.res, connsToNodeIDMap(test.source))
		})
	}
}

type filterFunc func(info balancerConfig.Info, c conn.Info) bool

func (f filterFunc) Allow(info balancerConfig.Info, c conn.Info) bool {
	return f(info, c)
}

func (f filterFunc) String() string {
	return "Custom"
}

func TestSortPreferConnections(t *testing.T) {
	table := []struct {
		name          string
		source        []conn.Info
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Info
		fallback      []conn.Info
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
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2"}},
			},
			allowFallback: false,
			filter:        nil,
			prefer: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2"}},
			},
			fallback: nil,
		},
		{
			name: "FilterNoFallback",
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2"}},
			},
			allowFallback: false,
			filter: filterFunc(func(_ balancerConfig.Info, c conn.Info) bool {
				return strings.HasPrefix(c.Endpoint().Address(), "t")
			}),
			prefer: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2"}},
			},
			fallback: nil,
		},
		{
			name: "FilterWithFallback",
			source: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2"}},
			},
			allowFallback: true,
			filter: filterFunc(func(_ balancerConfig.Info, c conn.Info) bool {
				return strings.HasPrefix(c.Endpoint().Address(), "t")
			}),
			prefer: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2"}},
			},
			fallback: []conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2"}},
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
	r := xrand.New(xrand.WithLock())

	t.Run("Empty", func(t *testing.T) {
		c, failedCount, has := selectRandomConnection[conn.Info](r, nil, true)
		require.False(t, has)
		require.Nil(t, c)
		require.Equal(t, 0, failedCount)
	})

	t.Run("One", func(t *testing.T) {
		for _, goodState := range []conn.State{connectivity.Ready, connectivity.Idle, connectivity.Connecting} {
			c, failedCount, has := selectRandomConnection(r,
				[]conn.Info{
					&mock.Conn{
						EndpointField: &mock.Endpoint{AddressField: "asd"},
						StateField:    goodState,
					},
				}, true,
			)
			require.True(t, has)
			require.NotNil(t, c)
			require.Equal(t, &mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "asd"},
				StateField:    goodState,
			}, c)
			require.Equal(t, 0, failedCount)
		}
	})
	t.Run("OneBanned", func(t *testing.T) {
		c, failedCount, has := selectRandomConnection(r,
			[]conn.Info{&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "asd"},
				StateField:    connectivity.TransientFailure,
			}}, false,
		)
		require.False(t, has)
		require.Nil(t, c)
		require.Equal(t, 1, failedCount)

		c, failedCount, has = selectRandomConnection(r,
			[]conn.Info{&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "asd"},
				StateField:    connectivity.TransientFailure,
			}}, true,
		)
		require.True(t, has)
		require.Equal(t, &mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "asd"},
			StateField:    connectivity.TransientFailure,
		}, c)
		require.Equal(t, 0, failedCount)
	})
	t.Run("Two", func(t *testing.T) {
		conns := []conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "1"},
				StateField:    connectivity.Ready,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "2"},
				StateField:    connectivity.Ready,
			},
		}
		first := 0
		second := 0
		for i := 0; i < 100; i++ {
			c, _, has := selectRandomConnection(r, conns, false)
			require.True(t, has)
			require.NotNil(t, c)
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
		conns := []conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "1"},
				StateField:    connectivity.TransientFailure,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "2"},
				StateField:    connectivity.TransientFailure,
			},
		}
		totalFailed := 0
		for i := 0; i < 100; i++ {
			c, failed, has := selectRandomConnection(r, conns, false)
			require.False(t, has)
			require.Nil(t, c)
			totalFailed += failed
		}
		require.Equal(t, 200, totalFailed)
	})
	t.Run("ThreeWithBanned", func(t *testing.T) {
		conns := []conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "1"},
				StateField:    connectivity.Ready,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "2"},
				StateField:    connectivity.Ready,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "3"},
				StateField:    connectivity.TransientFailure,
			},
		}
		first := 0
		second := 0
		failed := 0
		for i := 0; i < 100; i++ {
			c, checkFailed, has := selectRandomConnection(r, conns, false)
			failed += checkFailed
			require.True(t, has)
			require.NotNil(t, c)
			switch c.Endpoint().Address() {
			case "1":
				first++
			case "2":
				second++
			default:
				t.Errorf(c.Endpoint().Address())
			}
		}
		require.Equal(t, 100, first+second)
		require.InDelta(t, 50, first, 21)
		require.InDelta(t, 50, second, 21)
		require.Greater(t, 10, failed)
	})
}

func TestNewConnections(t *testing.T) {
	table := []struct {
		name  string
		state *connections[conn.Info]
		res   *connections[conn.Info]
	}{
		{
			name:  "Empty",
			state: newConnections[conn.Info](nil, nil, balancerConfig.Info{}, false),
			res: &connections[conn.Info]{
				connByNodeID: nil,
				prefer:       nil,
				fallback:     nil,
				all:          nil,
			},
		},
		{
			name: "NoFilter",
			state: newConnections([]conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
			}, nil, balancerConfig.Info{}, false),
			res: &connections[conn.Info]{
				connByNodeID: map[uint32]conn.Info{
					1: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
					2: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
				},
				prefer: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
				},
				fallback: nil,
				all: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
				},
			},
		},
		{
			name: "FilterDenyFallback",
			state: newConnections([]conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
			}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
				return info.SelfLocation == c.Endpoint().Location()
			}), balancerConfig.Info{SelfLocation: "t"}, false),
			res: &connections[conn.Info]{
				connByNodeID: map[uint32]conn.Info{
					1: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					2: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					3: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
					4: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
				prefer: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				},
				fallback: nil,
				all: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				},
			},
		},
		{
			name: "FilterAllowFallback",
			state: newConnections([]conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
			}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
				return info.SelfLocation == c.Endpoint().Location()
			}), balancerConfig.Info{SelfLocation: "t"}, true),
			res: &connections[conn.Info]{
				connByNodeID: map[uint32]conn.Info{
					1: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					2: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					3: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
					4: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
				prefer: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				},
				fallback: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
				all: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
			},
		},
		{
			name: "WithNodeID",
			state: newConnections([]conn.Info{
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
			}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
				return info.SelfLocation == c.Endpoint().Location()
			}), balancerConfig.Info{SelfLocation: "t"}, true),
			res: &connections[conn.Info]{
				connByNodeID: map[uint32]conn.Info{
					1: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					2: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					3: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
					4: &mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
				prefer: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
				},
				fallback: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
				},
				all: []conn.Info{
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t1", NodeIDField: 1, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f1", NodeIDField: 2, LocationField: "f"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "t2", NodeIDField: 3, LocationField: "t"}},
					&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "f2", NodeIDField: 4, LocationField: "f"}},
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
		s := newConnections[conn.Info](nil, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConn(context.Background())
		require.Nil(t, c)
		require.Equal(t, 0, failed)
	})
	t.Run("AllGood", func(t *testing.T) {
		s := newConnections([]conn.Info{
			&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1"}, StateField: connectivity.Ready},
			&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2"}, StateField: connectivity.Ready},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConn(context.Background())
		require.NotNil(t, c)
		require.Equal(t, 0, failed)
	})
	t.Run("WithBanned", func(t *testing.T) {
		s := newConnections([]conn.Info{
			&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1"}, StateField: connectivity.Ready},
			&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2"}, StateField: connectivity.TransientFailure},
		}, nil, balancerConfig.Info{}, false)
		c, _ := s.GetConn(context.Background())
		require.Equal(t, &mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "1"},
			StateField:    connectivity.Ready,
		}, c)
	})
	t.Run("AllBanned", func(t *testing.T) {
		s := newConnections([]conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "t1", LocationField: "t"},
				StateField:    connectivity.TransientFailure,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "f2", LocationField: "f"},
				StateField:    connectivity.TransientFailure,
			},
		}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
			return c.Endpoint().Location() == info.SelfLocation
		}), balancerConfig.Info{}, true)
		preferred := 0
		fallback := 0
		for i := 0; i < 100; i++ {
			c, failed := s.GetConn(context.Background())
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
		s := newConnections([]conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "t1", LocationField: "t"},
				StateField:    connectivity.TransientFailure,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "f2", LocationField: "f"},
				StateField:    connectivity.Ready,
			},
		}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
			return c.Endpoint().Location() == info.SelfLocation
		}), balancerConfig.Info{SelfLocation: "t"}, true)
		c, failed := s.GetConn(context.Background())
		require.Equal(t, &mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "f2", LocationField: "f"},
			StateField:    connectivity.Ready,
		}, c)
		require.Equal(t, 1, failed)
	})
	t.Run("PreferNodeID", func(t *testing.T) {
		s := newConnections([]conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1},
				StateField:    connectivity.Ready,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2},
				StateField:    connectivity.Ready,
			},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConn(WithEndpoint(
			context.Background(),
			&mock.Endpoint{AddressField: "2", NodeIDField: 2},
		))
		require.Equal(t, &mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2},
			StateField:    connectivity.Ready,
		}, c)
		require.Equal(t, 0, failed)
	})
	t.Run("PreferNodeIDWithBadState", func(t *testing.T) {
		s := newConnections([]conn.Info{
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1},
				StateField:    connectivity.Ready,
			},
			&mock.Conn{
				EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2},
				StateField:    connectivity.TransientFailure,
			},
		}, nil, balancerConfig.Info{}, false)
		c, failed := s.GetConn(WithEndpoint(context.Background(), &mock.Endpoint{AddressField: "2", NodeIDField: 2}))
		require.Equal(t, &mock.Conn{
			EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1},
			StateField:    connectivity.Ready,
		}, c)
		require.Equal(t, 0, failed)
	})
}

func TestWithBadConn(t *testing.T) {
	s := newConnections([]conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", NodeIDField: 3}},
	}, nil, balancerConfig.Info{}, false)
	require.Equal(t, connectionsSlice[conn.Info]{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", NodeIDField: 3}},
	}, s.all)
	require.Equal(t, connectionsSlice[conn.Info]{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", NodeIDField: 3}},
	}, s.prefer)
	require.Empty(t, s.fallback)
	s, has := s.withBadConn(&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}})
	require.True(t, has)
	require.Equal(t, newConnections([]conn.Info{
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "1", NodeIDField: 1}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "2", NodeIDField: 2}},
		&mock.Conn{EndpointField: &mock.Endpoint{AddressField: "3", NodeIDField: 3}},
	}, filterFunc(func(info balancerConfig.Info, c conn.Info) bool {
		return c.Endpoint().NodeID() != 1
	}), balancerConfig.Info{}, true), s)
}
