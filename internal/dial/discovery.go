package dial

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wg"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"sync"
	"time"
)

func (d *dialer) discover(ctx context.Context, c cluster.Cluster, conn conn.Conn, connConfig conn.Config) error {
	onDone := func() {
		_ = conn.Close()
		trace.DriverOnConnDrop(d.config.Trace, conn.Addr(), conn.Runtime().GetState())
	}
	discoveryClient := discovery.New(conn, d.config.Database, d.useTLS())

	curr, err := discoveryClient.Discover(ctx)
	if err != nil {
		onDone()
		return err
	}
	// Endpoints must be sorted to merge
	cluster.SortEndpoints(curr)
	wg := wg.New()
	wg.Add(len(curr))
	for _, e := range curr {
		go c.Insert(ctx, e, cluster.WithWG(wg), cluster.WithConnConfig(connConfig))
	}
	if d.config.FastDial {
		wg.WaitFirst()
	} else {
		wg.Wait()
	}
	c.SetExplorer(
		repeater.NewRepeater(
			d.config.DiscoveryInterval,
			func(ctx context.Context) {
				onDone := trace.DriverOnDiscovery(d.config.Trace, ctx)
				next, err := discoveryClient.Discover(ctx)
				endpoints := make(map[trace.Endpoint]trace.ConnState, len(next))
				for _, e := range next {
					if ok, stats := c.ConnStats(e.Addr); ok {
						endpoints[e] = stats.State
					}
				}
				onDone(endpoints, err)
				if err != nil {
					return
				}
				// if nothing endpoint - re-discover after one second
				// and use old endpoint list
				if len(next) == 0 {
					go func() {
						time.Sleep(time.Second)
						c.Force()

					}()
					return
				}
				// NOTE: curr endpoints must be sorted here.
				cluster.SortEndpoints(next)

				wg := new(sync.WaitGroup)
				max := len(next) + len(curr)
				wg.Add(max) // set to max possible amount
				actual := 0
				cluster.DiffEndpoints(curr, next,
					func(i, j int) {
						actual++
						// Endpoints are equal, but we still need to update meta
						// data such that load factor and others.
						go c.Update(ctx, next[j], cluster.WithWG(wg))
					},
					func(i, j int) {
						actual++
						go c.Insert(ctx, next[j], cluster.WithWG(wg), cluster.WithConnConfig(connConfig))
					},
					func(i, j int) {
						actual++
						go c.Remove(ctx, curr[i], cluster.WithWG(wg))
					},
				)
				wg.Add(actual - max) // adjust
				wg.Wait()
				curr = next
			},
			onDone,
		),
	)
	return nil
}
