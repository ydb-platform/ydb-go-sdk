package coordination

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

//nolint:gofumpt
//nolint:nolintlint
var (
	errNilClient = xerrors.Wrap(errors.New("coordination client is not initialized"))
)

type Client struct {
	config  config.Config
	service Ydb_Coordination_V1.CoordinationServiceClient
}

func New(ctx context.Context, cc grpc.ClientConnInterface, config config.Config) (*Client, error) {
	return &Client{
		config:  config,
		service: Ydb_Coordination_V1.NewCoordinationServiceClient(cc),
	}, nil
}

func (c *Client) CreateNode(ctx context.Context, path string, config coordination.NodeConfig) error {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.createNode(ctx, path, config))
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(call(ctx))
	}
	return retry.Retry(ctx,
		call, retry.WithStackTrace(),
		retry.WithIdempotent(true),
		retry.WithTrace(c.config.TraceRetry()),
	)
}

func (c *Client) createNode(ctx context.Context, path string, config coordination.NodeConfig) error {
	_, err := c.service.CreateNode(
		ctx,
		&Ydb_Coordination.CreateNodeRequest{
			Path: path,
			Config: &Ydb_Coordination.Config{
				Path:                     config.Path,
				SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
				SessionGracePeriodMillis: config.SessionGracePeriodMillis,
				ReadConsistencyMode:      config.ReadConsistencyMode.To(),
				AttachConsistencyMode:    config.AttachConsistencyMode.To(),
				RateLimiterCountersMode:  config.RatelimiterCountersMode.To(),
			},
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) AlterNode(ctx context.Context, path string, config coordination.NodeConfig) error {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.alterNode(ctx, path, config))
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(call(ctx))
	}
	return retry.Retry(ctx,
		call,
		retry.WithStackTrace(),
		retry.WithIdempotent(true),
		retry.WithTrace(c.config.TraceRetry()),
	)
}

func (c *Client) alterNode(ctx context.Context, path string, config coordination.NodeConfig) error {
	_, err := c.service.AlterNode(
		ctx,
		&Ydb_Coordination.AlterNodeRequest{
			Path: path,
			Config: &Ydb_Coordination.Config{
				Path:                     config.Path,
				SelfCheckPeriodMillis:    config.SelfCheckPeriodMillis,
				SessionGracePeriodMillis: config.SessionGracePeriodMillis,
				ReadConsistencyMode:      config.ReadConsistencyMode.To(),
				AttachConsistencyMode:    config.AttachConsistencyMode.To(),
				RateLimiterCountersMode:  config.RatelimiterCountersMode.To(),
			},
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) DropNode(ctx context.Context, path string) error {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) error {
		return xerrors.WithStackTrace(c.dropNode(ctx, path))
	}
	if !c.config.AutoRetry() {
		return xerrors.WithStackTrace(call(ctx))
	}
	return retry.Retry(ctx, call,
		retry.WithStackTrace(),
		retry.WithIdempotent(true),
		retry.WithTrace(c.config.TraceRetry()),
	)
}

func (c *Client) dropNode(ctx context.Context, path string) error {
	_, err := c.service.DropNode(
		ctx,
		&Ydb_Coordination.DropNodeRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) DescribeNode(
	ctx context.Context,
	path string,
) (
	entry *scheme.Entry,
	config *coordination.NodeConfig,
	_ error,
) {
	if c == nil {
		return nil, nil, xerrors.WithStackTrace(errNilClient)
	}
	call := func(ctx context.Context) (err error) {
		entry, config, err = c.describeNode(ctx, path)
		return xerrors.WithStackTrace(err)
	}
	if !c.config.AutoRetry() {
		err := call(ctx)
		return entry, config, xerrors.WithStackTrace(err)
	}
	err := retry.Retry(ctx, call,
		retry.WithStackTrace(),
		retry.WithIdempotent(true),
		retry.WithTrace(c.config.TraceRetry()),
	)
	return entry, config, xerrors.WithStackTrace(err)
}

// DescribeNode describes a coordination node
func (c *Client) describeNode(
	ctx context.Context,
	path string,
) (
	_ *scheme.Entry,
	_ *coordination.NodeConfig,
	err error,
) {
	var (
		response *Ydb_Coordination.DescribeNodeResponse
		result   Ydb_Coordination.DescribeNodeResult
	)
	response, err = c.service.DescribeNode(
		ctx,
		&Ydb_Coordination.DescribeNodeRequest{
			Path: path,
			OperationParams: operation.Params(
				ctx,
				c.config.OperationTimeout(),
				c.config.OperationCancelAfter(),
				operation.ModeSync,
			),
		},
	)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	err = response.GetOperation().GetResult().UnmarshalTo(&result)
	if err != nil {
		return nil, nil, xerrors.WithStackTrace(err)
	}
	return scheme.InnerConvertEntry(result.GetSelf()), &coordination.NodeConfig{
		Path:                     result.GetConfig().GetPath(),
		SelfCheckPeriodMillis:    result.GetConfig().GetSelfCheckPeriodMillis(),
		SessionGracePeriodMillis: result.GetConfig().GetSessionGracePeriodMillis(),
		ReadConsistencyMode:      consistencyMode(result.GetConfig().GetReadConsistencyMode()),
		AttachConsistencyMode:    consistencyMode(result.GetConfig().GetAttachConsistencyMode()),
		RatelimiterCountersMode:  ratelimiterCountersMode(result.GetConfig().GetRateLimiterCountersMode()),
	}, nil
}

func (c *Client) Close(ctx context.Context) error {
	if c == nil {
		return xerrors.WithStackTrace(errNilClient)
	}
	return c.close(ctx)
}

func (c *Client) close(context.Context) error {
	return nil
}

func consistencyMode(t Ydb_Coordination.ConsistencyMode) coordination.ConsistencyMode {
	switch t {
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:
		return coordination.ConsistencyModeStrict
	case Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED:
		return coordination.ConsistencyModeRelaxed
	default:
		return coordination.ConsistencyModeUnset
	}
}

func ratelimiterCountersMode(t Ydb_Coordination.RateLimiterCountersMode) coordination.RatelimiterCountersMode {
	switch t {
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED:
		return coordination.RatelimiterCountersModeAggregated
	case Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:
		return coordination.RatelimiterCountersModeDetailed
	default:
		return coordination.RatelimiterCountersModeUnset
	}
}
