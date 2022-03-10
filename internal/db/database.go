package db

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	builder "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type database struct {
	config    config.Config
	cluster   cluster.Cluster
	discovery discovery.Client
}

func (db *database) Discovery() discovery.Client {
	return db.discovery
}

func (db *database) GetConn(endpoint endpoint.Endpoint) conn.Conn {
	return db.cluster.GetConn(endpoint)
}

func (db *database) Close(ctx context.Context) (err error) {
	issues := make([]error, 0, 2)

	if err = db.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if err = db.cluster.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return errors.NewWithIssues("db close failed", issues...)
	}

	return nil
}

func New(
	ctx context.Context,
	c config.Config,
	pool conn.Pool,
	opts ...discoveryConfig.Option,
) (_ Connection, err error) {
	ctx, err = c.Meta().Meta(ctx)
	if err != nil {
		return nil, err
	}

	t := trace.ContextDriver(ctx).Compose(c.Trace())
	onDone := trace.DriverOnInit(t, &ctx, c.Endpoint(), c.Database(), c.Secure())
	defer func() {
		onDone(err)
	}()

	db := &database{
		config:  c,
		cluster: cluster.New(ctx, c, pool, c.Balancer()),
	}

	var cancel context.CancelFunc
	if t := c.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	db.discovery, err = builder.New(
		ctx,
		db.cluster.GetConn(endpoint.New(c.Endpoint(), endpoint.WithLocalDC(true))),
		db.cluster,
		db.config.Trace(),
		opts...,
	)
	if err != nil {
		return nil, err
	}

	return db, nil
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
		return errors.Errorf(0, "database.Invoke(%v, %v, %v, %v): %w",
			method,
			args,
			reply,
			opts,
			err,
		)
	}
	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return errors.Errorf(0, "database.Invoke(%v, %v, %v, %v): %w",
			method,
			args,
			reply,
			opts,
			err,
		)
	}
	defer func() {
		if err != nil && errors.MustPessimizeEndpoint(err) {
			db.cluster.Pessimize(ctx, cc, err)
		}
	}()
	err = cc.Invoke(ctx, method, args, reply, opts...)
	if err != nil {
		return errors.Errorf(0, "database.Invoke(%v, %v, %v, %v): %w",
			method,
			args,
			reply,
			opts,
			err,
		)
	}
	return nil
}

func (db *database) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cc, err := db.cluster.Get(ctx)
	if err != nil {
		return nil, errors.Errorf(0, "database.NewStream(%v, %v, %v): %w",
			desc,
			method,
			opts,
			err,
		)
	}
	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return nil, errors.Errorf(0, "database.NewStream(%v, %v, %v): %w",
			desc,
			method,
			opts,
			err,
		)
	}
	defer func() {
		if err != nil && errors.MustPessimizeEndpoint(err) {
			db.cluster.Pessimize(ctx, cc, err)
		}
	}()
	var client grpc.ClientStream
	client, err = cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, errors.Errorf(0, "database.NewStream(%v, %v, %v, %v): %w", ctx, desc, method, opts, err)
	}
	return client, nil
}
