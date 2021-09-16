package ydb

import (
	"context"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/yandex-cloud/ydb-go-sdk/v2/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/v2/internal"
)

func invoke(
	ctx context.Context, conn *grpc.ClientConn,
	resp internal.Response,
	method string, req, res proto.Message,
	opts ...grpc.CallOption,
) (
	err error,
) {
	if conn == nil {
		return ErrNilConnection
	}
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
	if res == nil {
		// NOTE: YDB API at this moment supports extension of its protocol by
		// adding Result structures. That is, one may think that no result is
		// provided by some call, but some day it may change and client
		// implementation will lag some time – no strict behavior is possible.
		return nil
	}
	return proto.Unmarshal(resp.GetResult().Value, res)
}
