package operation

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Operation_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

type (
	// Client is an operation service client for manage long operations in YDB
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Client struct {
		operationServiceClient Ydb_Operation_V1.OperationServiceClient
	}
	// Operation describes operation
	//
	// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
	Operation struct {
		ID            string
		Ready         bool
		Status        string
		ConsumedUnits float64
	}
)

// Get returns operation status by ID
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (c *Client) Get(ctx context.Context, opID string) (*Operation, error) {
	op, err := get(ctx, c.operationServiceClient, opID)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return op, nil
}

func get(
	ctx context.Context, client Ydb_Operation_V1.OperationServiceClient, opID string,
) (*Operation, error) {
	status, err := retry.RetryWithResult(ctx, func(ctx context.Context) (*Operation, error) {
		response, err := client.GetOperation(
			conn.WithoutWrapping(ctx),
			&Ydb_Operations.GetOperationRequest{
				Id: opID,
			},
		)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		var md Ydb_Query.ExecuteScriptMetadata
		err = response.GetOperation().GetMetadata().UnmarshalTo(&md)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return &Operation{
			Ready:  response.GetOperation().GetReady(),
			Status: response.GetOperation().GetStatus().String(),
		}, nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return status, nil
}

func list(
	ctx context.Context, client Ydb_Operation_V1.OperationServiceClient, request *Ydb_Operations.ListOperationsRequest,
) ([]*Operation, error) {
	operations, err := retry.RetryWithResult(ctx, func(ctx context.Context) (operations []*Operation, _ error) {
		response, err := client.ListOperations(conn.WithoutWrapping(ctx), request)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return nil, xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(response.GetStatus()),
				xerrors.WithIssues(response.GetIssues()),
			))
		}

		for _, op := range response.GetOperations() {
			operations = append(operations, &Operation{
				ID:            op.GetId(),
				Ready:         op.GetReady(),
				Status:        op.GetStatus().String(),
				ConsumedUnits: op.GetCostInfo().GetConsumedUnits(),
			})
		}

		return operations, nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return operations, nil
}

// List returns list of operations that match the specified filter in the request.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (c *Client) List(ctx context.Context, opts ...options.List) ([]*Operation, error) {
	request := &options.ListOperationsRequest{}
	for _, opt := range opts {
		if opt != nil {
			opt(request)
		}
	}

	operations, err := list(ctx, c.operationServiceClient, &request.ListOperationsRequest)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return operations, nil
}

func cancel(
	ctx context.Context, client Ydb_Operation_V1.OperationServiceClient, opID string,
) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		response, err := client.CancelOperation(conn.WithoutWrapping(ctx), &Ydb_Operations.CancelOperationRequest{
			Id: opID,
		})
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(response.GetStatus()),
				xerrors.WithIssues(response.GetIssues()),
			))
		}

		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// Cancel starts cancellation of a long-running operation.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (c *Client) Cancel(ctx context.Context, opID string) error {
	err := cancel(ctx, c.operationServiceClient, opID)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func forget(
	ctx context.Context, client Ydb_Operation_V1.OperationServiceClient, opID string,
) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		response, err := client.ForgetOperation(conn.WithoutWrapping(ctx), &Ydb_Operations.ForgetOperationRequest{
			Id: opID,
		})
		if err != nil {
			return xerrors.WithStackTrace(err)
		}

		if response.GetStatus() != Ydb.StatusIds_SUCCESS {
			return xerrors.WithStackTrace(xerrors.Operation(
				xerrors.WithStatusCode(response.GetStatus()),
				xerrors.WithIssues(response.GetIssues()),
			))
		}

		return nil
	}, retry.WithIdempotent(true))
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

// Forget forgets long-running operation. It does not cancel the operation and
// returns an error if operation was not completed.
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func (c *Client) Forget(ctx context.Context, opID string) error {
	err := forget(ctx, c.operationServiceClient, opID)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (c *Client) Close(ctx context.Context) error {
	return nil
}

func New(ctx context.Context, balancer grpc.ClientConnInterface) *Client {
	return &Client{
		operationServiceClient: Ydb_Operation_V1.NewOperationServiceClient(
			conn.WithContextModifier(balancer, conn.WithoutWrapping),
		),
	}
}
