package cluster

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClusterMergeEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Cluster{
		config: config.New(
			config.WithBalancer(func() balancer.Balancer {
				_, b := mock.Balancer()
				return b
			}()),
		),
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
		pool:      conn.NewPool(ctx, config.New()),
	}

	assert := func(t *testing.T, exp []endpoint.Endpoint) {
		if len(c.index) != len(exp) {
			t.Fatalf("unexpected number of endpoints %d: got %d", len(exp), len(c.index))
		}
		for _, e := range exp {
			if _, ok := c.index[e.Address()]; !ok {
				t.Fatalf("not found endpoint '%v' in index", e.Address())
			}
		}
		for _, entry := range c.index {
			if func() bool {
				for _, e := range exp {
					if e.Address() == entry.Conn.Endpoint().Address() {
						return false
					}
				}
				return true
			}() {
				t.Fatalf("unexpected endpoint '%v' in index", entry.Conn.Endpoint().Address())
			}
		}
	}

	endpoints := []endpoint.Endpoint{
		endpoint.New("foo:0"),
		endpoint.New("foo:123"),
	}
	badEndpoints := []endpoint.Endpoint{
		endpoint.New("baz:0"),
		endpoint.New("baz:123"),
	}
	nextEndpoints := []endpoint.Endpoint{
		endpoint.New("foo:0"),
		endpoint.New("bar:0"),
		endpoint.New("bar:123"),
	}
	nextBadEndpoints := []endpoint.Endpoint{
		endpoint.New("bad:23"),
	}
	t.Run("InitialFill", func(t *testing.T) {
		// nolint:gocritic
		// nolint:nolintlint
		ne := append(endpoints, badEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			[]endpoint.Endpoint{},
			ne,
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("UpdateWithAnotherEndpoints", func(t *testing.T) {
		// nolint:gocritic
		// nolint:nolintlint
		ne := append(nextEndpoints, nextBadEndpoints...)
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			append(endpoints, badEndpoints...),
			ne,
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("LeftOnlyBad", func(t *testing.T) {
		ne := nextBadEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			append(nextEndpoints, nextBadEndpoints...),
			ne,
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
	t.Run("LeftOnlyGood", func(t *testing.T) {
		ne := nextEndpoints
		// merge new endpoints into balancer
		mergeEndpointIntoCluster(
			ctx,
			c,
			nextBadEndpoints,
			ne,
		)
		// try endpoints, filter out bad ones to tracking
		assert(t, ne)
	})
}

func TestDiffEndpoint(t *testing.T) {
	// lists must be sorted
	var noEndpoints []endpoint.Endpoint
	someEndpoints := []endpoint.Endpoint{
		endpoint.New("0:0"),
		endpoint.New("1:1"),
	}
	sameSomeEndpoints := []endpoint.Endpoint{
		endpoint.New("0:0", endpoint.WithLoadFactor(1), endpoint.WithLocalDC(true)),
		endpoint.New("1:1", endpoint.WithLoadFactor(2), endpoint.WithLocalDC(true)),
	}
	anotherEndpoints := []endpoint.Endpoint{
		endpoint.New("2:0"),
		endpoint.New("3:1"),
	}
	moreEndpointsOverlap := []endpoint.Endpoint{
		endpoint.New("0:0", endpoint.WithLoadFactor(1), endpoint.WithLocalDC(true)),
		endpoint.New("1:1"),
		endpoint.New("1:2"),
	}

	type TC struct {
		name         string
		curr, next   []endpoint.Endpoint
		eq, add, del int
	}

	tests := []TC{
		{
			name: "none",
			curr: noEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  0,
		},
		{
			name: "equals",
			curr: someEndpoints,
			next: sameSomeEndpoints,
			eq:   2,
			add:  0,
			del:  0,
		},
		{
			name: "noneToSome",
			curr: noEndpoints,
			next: someEndpoints,
			eq:   0,
			add:  2,
			del:  0,
		},
		{
			name: "SomeToNone",
			curr: someEndpoints,
			next: noEndpoints,
			eq:   0,
			add:  0,
			del:  2,
		},
		{
			name: "SomeToMore",
			curr: someEndpoints,
			next: moreEndpointsOverlap,
			eq:   2,
			add:  1,
			del:  0,
		},
		{
			name: "MoreToSome",
			curr: moreEndpointsOverlap,
			next: someEndpoints,
			eq:   2,
			add:  0,
			del:  1,
		},
		{
			name: "SomeToAnother",
			curr: someEndpoints,
			next: anotherEndpoints,
			eq:   0,
			add:  2,
			del:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eq, add, del := 0, 0, 0
			DiffEndpoints(tc.curr, tc.next,
				func(i, j int) { eq++ },
				func(i, j int) { add++ },
				func(i, j int) { del++ },
			)
			if eq != tc.eq || add != tc.add || del != tc.del {
				t.Errorf("Got %d, %d, %d expected: %d, %d, %d", eq, add, del, tc.eq, tc.add, tc.del)
			}
		})
	}
}

func TestEndpointSwitchLocalDCFlag(t *testing.T) {
	var (
		ctx               = context.Background()
		curr              []endpoint.Endpoint
		clusterEndpoints  = make(map[string]struct{})
		balancerEndpoints = make(map[string]struct{})
		c                 = New(
			ctx,
			config.New(
				config.WithBalancer(
					balancers.PreferLocalDC(
						balancers.RoundRobin(),
					),
				),
				config.WithTrace(trace.Driver{
					OnClusterInsert: func(info trace.DriverClusterInsertStartInfo) func(trace.DriverClusterInsertDoneInfo) {
						address := info.Endpoint.Address()
						return func(info trace.DriverClusterInsertDoneInfo) {
							clusterEndpoints[address] = struct{}{}
							if info.Inserted {
								balancerEndpoints[address] = struct{}{}
							}
						}
					},
					OnClusterRemove: func(info trace.DriverClusterRemoveStartInfo) func(trace.DriverClusterRemoveDoneInfo) {
						address := info.Endpoint.Address()
						return func(info trace.DriverClusterRemoveDoneInfo) {
							delete(clusterEndpoints, address)
							if info.Removed {
								delete(balancerEndpoints, address)
							}
						}
					},
				}),
			),
			conn.NewPool(ctx, config.New()),
			nil, // TODO: actualize
		)
	)
	for _, test := range []struct {
		name                      string
		next                      []endpoint.Endpoint
		expectedClusterEndpoints  []endpoint.Endpoint
		expectedBalancerEndpoints []endpoint.Endpoint
	}{
		{
			name: "init",
			next: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
				endpoint.New("1:1", endpoint.WithLocalDC(false)),
			},
			expectedClusterEndpoints: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
				endpoint.New("1:1", endpoint.WithLocalDC(false)),
			},
			expectedBalancerEndpoints: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
			},
		},
		{
			name: "first switch localDC flag",
			next: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(false)),
				endpoint.New("1:1", endpoint.WithLocalDC(true)),
			},
			expectedClusterEndpoints: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(false)),
				endpoint.New("1:1", endpoint.WithLocalDC(true)),
			},
			expectedBalancerEndpoints: []endpoint.Endpoint{
				endpoint.New("1:1", endpoint.WithLocalDC(true)),
			},
		},
		{
			name: "second switch localDC flag",
			next: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
				endpoint.New("1:1", endpoint.WithLocalDC(false)),
			},
			expectedClusterEndpoints: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
				endpoint.New("1:1", endpoint.WithLocalDC(false)),
			},
			expectedBalancerEndpoints: []endpoint.Endpoint{
				endpoint.New("0:0", endpoint.WithLocalDC(true)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			DiffEndpoints(
				curr,
				test.next,
				func(i, j int) {
					c.Remove(
						ctx,
						curr[i],
						WithoutLock(),
					)
					c.Insert(
						ctx,
						test.next[j],
						WithoutLock(),
					)
				},
				func(i, j int) {
					c.Insert(
						ctx,
						test.next[j],
						WithoutLock(),
					)
				},
				func(i, j int) {
					c.Remove(
						ctx,
						curr[i],
						WithoutLock(),
					)
				},
			)
			{
				v1 := fmt.Sprintf(
					"%v",
					func() (addrs []string) {
						for addr := range clusterEndpoints {
							addrs = append(addrs, addr)
						}
						sort.Strings(addrs)
						return addrs
					}(),
				)
				v2 := fmt.Sprintf(
					"%v",
					func() (addrs []string) {
						for _, e := range test.expectedClusterEndpoints {
							addrs = append(addrs, e.Address())
						}
						sort.Strings(addrs)
						return addrs
					}(),
				)
				if v1 != v2 {
					t.Fatalf("unexpected cluster endpoints: %v, exp: %v", v1, v2)
				}
			}
			{
				v1 := fmt.Sprintf(
					"%v",
					func() (addrs []string) {
						for addr := range balancerEndpoints {
							addrs = append(addrs, addr)
						}
						sort.Strings(addrs)
						return addrs
					}(),
				)
				v2 := fmt.Sprintf(
					"%v",
					func() (addrs []string) {
						for _, e := range test.expectedBalancerEndpoints {
							addrs = append(addrs, e.Address())
						}
						sort.Strings(addrs)
						return addrs
					}(),
				)
				if v1 != v2 {
					t.Fatalf("unexpected balancer endpoints: %v, exp: %v", v1, v2)
				}
			}
			curr = test.next
		})
	}
}
