package dial

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/wg"
)

func (d *dialer) discover(ctx context.Context, c cluster.Cluster, conn conn.Conn, connConfig conn.Config) error {
	discoveryClient := discovery.New(conn, d.config.Database(), d.config.Secure(), d.config.Trace())

	curr, err := discoveryClient.Discover(ctx)
	if err != nil {
		_ = conn.Close(ctx)
		return err
	}
	// Endpoints must be sorted to merge
	cluster.SortEndpoints(curr)
	waitGroup := wg.New()
	waitGroup.Add(len(curr))
	for _, e := range curr {
		go c.Insert(
			ctx,
			e,
			cluster.WithWG(waitGroup),
			cluster.WithConnConfig(connConfig),
		)
	}
	if d.config.FastDial() {
		waitGroup.WaitFirst()
	} else {
		waitGroup.Wait()
	}
	c.SetExplorer(
		repeater.NewRepeater(
			ctx,
			d.config.DiscoveryInterval(),
			func(ctx context.Context) {
				next, err := discoveryClient.Discover(ctx)
				// if nothing endpoint - re-discover after one second
				// and use old endpoint list
				if err != nil || len(next) == 0 {
					go func() {
						time.Sleep(time.Second)
						c.Force()
					}()
					return
				}
				// NOTE: curr endpoints must be sorted here.
				cluster.SortEndpoints(next)

				waitGroup := new(sync.WaitGroup)
				max := len(next) + len(curr)
				waitGroup.Add(max) // set to max possible amount
				actual := 0
				cluster.DiffEndpoints(curr, next,
					func(i, j int) {
						actual++
						// Endpoints are equal, but we still need to update meta
						// data such that load factor and others.
						go c.Update(
							ctx,
							next[j],
							cluster.WithWG(waitGroup),
						)
					},
					func(i, j int) {
						actual++
						go c.Insert(
							ctx,
							next[j],
							cluster.WithWG(waitGroup),
							cluster.WithConnConfig(connConfig),
						)
					},
					func(i, j int) {
						actual++
						go c.Remove(
							ctx,
							curr[i],
							cluster.WithWG(waitGroup),
						)
					},
				)
				waitGroup.Add(actual - max) // adjust
				waitGroup.Wait()
				curr = next
			},
			func() {
				_ = conn.Close(ctx)
			},
		),
	)
	return nil
}
