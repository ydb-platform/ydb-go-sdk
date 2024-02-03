package balancer

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
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

type filterFunc func(info balancerConfig.Info, c conn.Conn) bool

func (f filterFunc) Allow(info balancerConfig.Info, c conn.Conn) bool {
	return f(info, c)
}

func (f filterFunc) String() string {
	return "Custom"
}

func TestSortPreferConnections_Empty(t *testing.T) {
	test := struct {
		name          string
		source        []conn.Conn
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Conn
		fallback      []conn.Conn
	}{
		name:          "Empty",
		source:        nil,
		allowFallback: false,
		filter:        nil,
		prefer:        nil,
		fallback:      nil,
	}

	prefer, fallback := sortPreferConnections(test.source, test.filter, balancerConfig.Info{}, test.allowFallback)
	require.Equal(t, test.prefer, prefer)
	require.Equal(t, test.fallback, fallback)
}

func TestSortPreferConnections_NilFilter(t *testing.T) {
	test := struct {
		name          string
		source        []conn.Conn
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Conn
		fallback      []conn.Conn
	}{
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
	}

	prefer, fallback := sortPreferConnections(test.source, test.filter, balancerConfig.Info{}, test.allowFallback)
	require.Equal(t, test.prefer, prefer)
	require.Equal(t, test.fallback, fallback)
}

func TestSortPreferConnections_FilterNoFallback(t *testing.T) {
	test := struct {
		name          string
		source        []conn.Conn
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Conn
		fallback      []conn.Conn
	}{
		name: "FilterNoFallback",
		source: []conn.Conn{
			&mock.Conn{AddrField: "t1"},
			&mock.Conn{AddrField: "f1"},
			&mock.Conn{AddrField: "t2"},
			&mock.Conn{AddrField: "f2"},
		},
		allowFallback: false,
		filter: filterFunc(func(_ balancerConfig.Info, c conn.Conn) bool {
			return strings.HasPrefix(c.Endpoint().Address(), "t")
		}),
		prefer: []conn.Conn{
			&mock.Conn{AddrField: "t1"},
			&mock.Conn{AddrField: "t2"},
		},
		fallback: nil,
	}

	prefer, fallback := sortPreferConnections(test.source, test.filter, balancerConfig.Info{}, test.allowFallback)
	require.Equal(t, test.prefer, prefer)
	require.Equal(t, test.fallback, fallback)
}

func TestSortPreferConnections_FilterWithFallback(t *testing.T) {
	test := struct {
		name          string
		source        []conn.Conn
		allowFallback bool
		filter        balancerConfig.Filter
		prefer        []conn.Conn
		fallback      []conn.Conn
	}{
		name: "FilterWithFallback",
		source: []conn.Conn{
			&mock.Conn{AddrField: "t1"},
			&mock.Conn{AddrField: "f1"},
			&mock.Conn{AddrField: "t2"},
			&mock.Conn{AddrField: "f2"},
		},
		allowFallback: true,
		filter: filterFunc(func(_ balancerConfig.Info, c conn.Conn) bool {
			return strings.HasPrefix(c.Endpoint().Address(), "t")
		}),
		prefer: []conn.Conn{
			&mock.Conn{AddrField: "t1"},
			&mock.Conn{AddrField: "t2"},
		},
		fallback: []conn.Conn{
			&mock.Conn{AddrField: "f1"},
			&mock.Conn{AddrField: "f2"},
		},
	}

	prefer, fallback := sortPreferConnections(test.source, test.filter, balancerConfig.Info{}, test.allowFallback)
	require.Equal(t, test.prefer, prefer)
	require.Equal(t, test.fallback, fallback)
}

func TestSelectRandomConnection_Empty(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

	c, failedCount := s.selectRandomConnection(nil, false)
	require.Nil(t, c)
	require.Equal(t, 0, failedCount)
}

func TestSelectRandomConnection_One(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

	for _, goodState := range []conn.State{conn.Online, conn.Offline, conn.Created} {
		c, failedCount := s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: goodState}}, false)
		require.Equal(t, &mock.Conn{AddrField: "asd", State: goodState}, c)
		require.Equal(t, 0, failedCount)
	}
}

func TestSelectRandomConnection_OneBanned(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

	c, failedCount := s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: conn.Banned}}, false)
	require.Nil(t, c)
	require.Equal(t, 1, failedCount)

	c, failedCount = s.selectRandomConnection([]conn.Conn{&mock.Conn{AddrField: "asd", State: conn.Banned}}, true)
	require.Equal(t, &mock.Conn{AddrField: "asd", State: conn.Banned}, c)
	require.Equal(t, 0, failedCount)
}

func TestSelectRandomConnection_Two(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

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
}

func TestSelectRandomConnection_TwoBanned(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

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
}

func TestSelectRandomConnection_ThreeWithBanned(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)

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
			t.Errorf(c.Endpoint().Address())
		}
	}
	require.Equal(t, 100, first+second)
	require.InDelta(t, 50, first, 21)
	require.InDelta(t, 50, second, 21)
	require.Greater(t, 10, failed)
}

func TestNewState_Empty(t *testing.T) {
	expectedResult := &connectionsState{
		connByNodeID: nil,
		prefer:       nil,
		fallback:     nil,
		all:          nil,
	}
	state := newConnectionsState(nil, nil, balancerConfig.Info{}, false)
	require.NotNil(t, state.rand)
	state.rand = nil
	require.Equal(t, expectedResult, state)
}

func TestNewState_NoFilter(t *testing.T) {
	expectedResult := &connectionsState{
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
	}
	state := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "1", NodeIDField: 1},
		&mock.Conn{AddrField: "2", NodeIDField: 2},
	}, nil, balancerConfig.Info{}, false)
	require.NotNil(t, state.rand)
	state.rand = nil
	require.Equal(t, expectedResult, state)
}

func TestNewState_FilterDenyFallback(t *testing.T) {
	expectedResult := &connectionsState{
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
	}
	state := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
		&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
		&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
		&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
	}, filterFunc(func(info balancerConfig.Info, c conn.Conn) bool {
		return info.SelfLocation == c.Endpoint().Location()
	}), balancerConfig.Info{SelfLocation: "t"}, false)
	require.NotNil(t, state.rand)
	state.rand = nil
	require.Equal(t, expectedResult, state)
}

func TestNewState_FilterAllowFallback(t *testing.T) {
	expectedResult := &connectionsState{
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
	}
	state := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
		&mock.Conn{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
		&mock.Conn{AddrField: "t2", NodeIDField: 3, LocationField: "t"},
		&mock.Conn{AddrField: "f2", NodeIDField: 4, LocationField: "f"},
	}, filterFunc(func(info balancerConfig.Info, c conn.Conn) bool {
		return info.SelfLocation == c.Endpoint().Location()
	}), balancerConfig.Info{SelfLocation: "t"}, true)
	require.NotNil(t, state.rand)
	state.rand = nil
	require.Equal(t, expectedResult, state)
}

func TestConnection_Empty(t *testing.T) {
	s := newConnectionsState(nil, nil, balancerConfig.Info{}, false)
	c, failed := s.GetConnection(context.Background())
	require.Nil(t, c)
	require.Equal(t, 0, failed)
}

func TestConnection_AllGood(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "1", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online},
	}, nil, balancerConfig.Info{}, false)
	c, failed := s.GetConnection(context.Background())
	require.NotNil(t, c)
	require.Equal(t, 0, failed)
}

func TestConnection_WithBanned(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "1", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Banned},
	}, nil, balancerConfig.Info{}, false)
	c, _ := s.GetConnection(context.Background())
	require.Equal(t, &mock.Conn{AddrField: "1", State: conn.Online}, c)
}

func TestConnection_AllBanned(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "t1", State: conn.Banned, LocationField: "t"},
		&mock.Conn{AddrField: "f2", State: conn.Banned, LocationField: "f"},
	}, filterFunc(func(info balancerConfig.Info, c conn.Conn) bool {
		return c.Endpoint().Location() == info.SelfLocation
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
}

func TestConnection_PreferBannedWithFallback(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "t1", State: conn.Banned, LocationField: "t"},
		&mock.Conn{AddrField: "f2", State: conn.Online, LocationField: "f"},
	}, filterFunc(func(info balancerConfig.Info, c conn.Conn) bool {
		return c.Endpoint().Location() == info.SelfLocation
	}), balancerConfig.Info{SelfLocation: "t"}, true)
	c, failed := s.GetConnection(context.Background())
	require.Equal(t, &mock.Conn{AddrField: "f2", State: conn.Online, LocationField: "f"}, c)
	require.Equal(t, 1, failed)
}

func TestConnection_PreferNodeID(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1},
		&mock.Conn{AddrField: "2", State: conn.Online, NodeIDField: 2},
	}, nil, balancerConfig.Info{}, false)
	c, failed := s.GetConnection(WithEndpoint(context.Background(), &mock.Endpoint{AddrField: "2", NodeIDField: 2}))
	require.Equal(t, &mock.Conn{AddrField: "2", State: conn.Online, NodeIDField: 2}, c)
	require.Equal(t, 0, failed)
}

func TestConnection_PreferNodeIDWithBadState(t *testing.T) {
	s := newConnectionsState([]conn.Conn{
		&mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1},
		&mock.Conn{AddrField: "2", State: conn.Unknown, NodeIDField: 2},
	}, nil, balancerConfig.Info{}, false)
	c, failed := s.GetConnection(WithEndpoint(context.Background(), &mock.Endpoint{AddrField: "2", NodeIDField: 2}))
	require.Equal(t, &mock.Conn{AddrField: "1", State: conn.Online, NodeIDField: 1}, c)
	require.Equal(t, 0, failed)
}
