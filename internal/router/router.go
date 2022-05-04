package router

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	clusterBuilder "github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/deadline"
	discoveryBuilder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type cluster interface {
	Close(ctx context.Context) error
	Get(ctx context.Context) (cc conn.Conn, err error)
	Ban(ctx context.Context, cc conn.Conn, cause error)
	Unban(ctx context.Context, cc conn.Conn)
}

type router struct {
	config config.Config
	pool   conn.Pool

	clusterMtx sync.RWMutex
	clusterPtr cluster

	discovery         discovery.Client
	discoveryRepeater repeater.Repeater
}

func (r *router) cluster() cluster {
	r.clusterMtx.RLock()
	defer r.clusterMtx.RUnlock()
	return r.clusterPtr
}

func (r *router) clusterCreate(ctx context.Context, endpoints []endpoint.Endpoint) cluster {
	return clusterBuilder.New(
		deadline.ContextWithoutDeadline(ctx),
		r.config,
		r.pool,
		endpoints,
		func(ctx context.Context) {
			r.discoveryRepeater.Force()
		})
}

func (r *router) clusterSwap(cluster cluster) cluster {
	r.clusterMtx.Lock()
	defer r.clusterMtx.Unlock()

	oldCluster := r.clusterPtr
	r.clusterPtr = cluster
	return oldCluster
}

func (r *router) clusterDiscovery(ctx context.Context) error {
	endpoints, err := r.discovery.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	newCluster := r.clusterCreate(ctx, endpoints)
	oldCluster := r.clusterSwap(newCluster)
	if oldCluster == nil {
		return nil
	}
	return oldCluster.Close(ctx)
}

func (r *router) Discovery() discovery.Client {
	return r.discovery
}

func (r *router) Close(ctx context.Context) (err error) {
	issues := make([]error, 0, 2)

	if r.discoveryRepeater != nil {
		r.discoveryRepeater.Stop()
	}

	if err = r.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	c := r.clusterSwap(nil)
	if c != nil {
		if err = c.Close(ctx); err != nil {
			issues = append(issues, err)
		}
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("router close failed", issues...))
	}

	return nil
}

func New(
	ctx context.Context,
	c config.Config,
	pool conn.Pool,
	opts ...discoveryConfig.Option,
) (_ Connection, err error) {
	onDone := trace.DriverOnInit(
		c.Trace(),
		&ctx,
		c.Endpoint(),
		c.Database(),
		c.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	db := &router{
		config: c,
		pool:   pool,
	}

	discoveryEndpoint := endpoint.New(c.Endpoint())
	discoveryConnection := pool.Get(discoveryEndpoint)

	discoveryConfig := discoveryConfig.New(opts...)

	db.discovery = discoveryBuilder.New(
		discoveryConnection,
		discoveryConfig,
	)

	if err = db.clusterDiscovery(ctx); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	if d := discoveryConfig.Interval(); d > 0 {
		db.discoveryRepeater = repeater.New(deadline.ContextWithoutDeadline(ctx), d, func(ctx context.Context) (err error) {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			return db.clusterDiscovery(ctx)
		},
			repeater.WithName("discovery"),
			repeater.WithTrace(db.config.Trace()),
		)
	} else {
		db.clusterSwap(db.clusterCreate(ctx, []endpoint.Endpoint{discoveryEndpoint}))
	}

	var cancel context.CancelFunc
	if t := c.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return db, nil
}

func (r *router) Endpoint() string {
	return r.config.Endpoint()
}

func (r *router) Name() string {
	return r.config.Database()
}

func (r *router) Secure() bool {
	return r.config.Secure()
}

func (r *router) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	cc, err := r.cluster().Get(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer r.handleConnRequestError(ctx, &err, cc)

	ctx, err = r.config.Meta().Meta(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = cc.Invoke(ctx, method, args, reply, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (r *router) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cc, err := r.cluster().Get(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	defer r.handleConnRequestError(ctx, &err, cc)

	ctx, err = r.config.Meta().Meta(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var client grpc.ClientStream
	client, err = cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return client, nil
}

func (r *router) handleConnRequestError(ctx context.Context, perr *error, cc conn.Conn) {
	err := *perr
	if err == nil && cc.GetState() == conn.Banned {
		r.cluster().Unban(ctx, cc)
	}
	if err != nil && xerrors.MustPessimizeEndpoint(err, r.config.ExcludeGRPCCodesForPessimization()...) {
		r.cluster().Ban(ctx, cc, err)
	}
}
