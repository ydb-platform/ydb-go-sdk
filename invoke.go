package ydb

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal"
)

func invoke(
	ctx context.Context, conn *grpc.ClientConn,
	resp internal.Response,
	method string, req, res proto.Message,
	opts ...grpc.CallOption,
) (
	err error,
) {
	err = conn.Invoke(ctx, method, req, resp.GetResponseProto(), opts...)
	switch {
	case err != nil:
		err = mapGRPCError(err)

	case !resp.GetOpReady():
		err = ErrOperationNotReady

	case resp.GetStatus() != Ydb.StatusIds_SUCCESS:
		err = &OpError{
			Reason: statusCode(resp.GetStatus()),
			issues: resp.GetIssues(),
		}
	}
	if err != nil {
		return err
	}
	return proto.Unmarshal(resp.GetResult().Value, res)
}
