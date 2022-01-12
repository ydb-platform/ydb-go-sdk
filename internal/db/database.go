package db

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/single"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/deadline"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type database struct {
	config  config.Config
	cluster cluster.Cluster
}

func (db *database) Close(ctx context.Context) error {
	return db.cluster.Close(ctx)
}

func New(
	ctx context.Context,
	cfg config.Config,
	pool conn.Pool,
) (_ Connection, err error) {
	if cfg.Endpoint() == "" {
		panic("empty dial address")
	}
	if cfg.Database() == "" {
		panic("empty database")
	}
	ctx, err = cfg.Meta().Meta(ctx)
	if err != nil {
		return nil, err
	}
	t := trace.ContextDriver(ctx).Compose(cfg.Trace())
	onDone := trace.DriverOnInit(t, &ctx, cfg.Endpoint(), cfg.Database(), cfg.Secure())
	defer func() {
		onDone(err)
	}()
	c := cluster.New(pool, t, cfg.Balancer())
	var cancel context.CancelFunc
	if t := cfg.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, cfg.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	if single.IsSingle(cfg.Balancer()) {
		c.Insert(
			ctx,
			endpoint.New(cfg.Endpoint(), endpoint.WithLocalDC(true)),
			cluster.WithConnConfig(cfg),
		)
	} else {
		err = discover(ctx, cfg, pool, c)
		if err != nil {
			return nil, err
		}
	}
	db := &database{
		config:  cfg,
		cluster: c,
	}
	return db, nil
}

func discover(ctx context.Context, cfg config.Config, pool conn.Pool, c cluster.Cluster) error {
	cc := pool.Get(endpoint.New(cfg.Endpoint()))
	t := trace.ContextDriver(ctx).Compose(cfg.Trace())
	client := discovery.New(cc, cfg.Endpoint(), cfg.Database(), cfg.Secure(), t)
	curr, err := client.Discover(ctx)
	if err != nil {
		_ = cc.Close(ctx)
		return err
	}
	// Endpoints must be sorted to merge
	cluster.SortEndpoints(curr)
	wg := &sync.WaitGroup{}
	wg.Add(len(curr))
	for _, e := range curr {
		go c.Insert(
			ctx,
			e,
			cluster.WithWG(wg),
			cluster.WithConnConfig(cfg),
		)
	}
	wg.Wait()
	c.SetExplorer(
		repeater.NewRepeater(
			deadline.ContextWithoutDeadline(ctx),
			cfg.DiscoveryInterval(),
			func(ctx context.Context) {
				next, err := client.Discover(ctx)
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
							cluster.WithConnConfig(cfg),
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
				_ = cc.Close(ctx)
			},
		),
	)
	return nil
}

func (db *database) Endpoint() string {
	return db.config.Endpoint()
}

func (db *database) Name() string {
	return db.config.Database()
}

func (db *database) Secure() bool {
	return db.config.Secure()
}

func (db *database) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	cc, err := db.cluster.Get(ctx)
	if err != nil {
		return err
	}
	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return err
	}
	return cc.Invoke(ctx, method, args, reply, opts...)
}

func (db *database) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cc, err := db.cluster.Get(ctx)
	if err != nil {
		return nil, err
	}
	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return nil, err
	}
	return cc.NewStream(ctx, desc, method, opts...)
}
