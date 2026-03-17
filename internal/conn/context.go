package conn

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type (
	ctxNoWrappingKey        struct{}
	ctxBanOnOperationError  struct{}
	ctxBanOnTransportError  struct{}
	operationErrorCodesType []Ydb.StatusIds_StatusCode
	transportErrorCodesType []grpcCodes.Code
)

func WithoutWrapping(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxNoWrappingKey{}, true)
}

func UseWrapping(ctx context.Context) bool {
	b, ok := ctx.Value(ctxNoWrappingKey{}).(bool)

	return !ok || !b
}

func BanOnOperationError(ctx context.Context, codes ...Ydb.StatusIds_StatusCode) context.Context {
	existingCodes, _ := ctx.Value(ctxBanOnOperationError{}).(operationErrorCodesType)
	existingCodes = append(existingCodes, codes...)

	return context.WithValue(ctx, ctxBanOnOperationError{}, existingCodes)
}

func BanOnTransportError(ctx context.Context, codes ...grpcCodes.Code) context.Context {
	existingCodes, _ := ctx.Value(ctxBanOnTransportError{}).(transportErrorCodesType)
	existingCodes = append(existingCodes, codes...)

	return context.WithValue(ctx, ctxBanOnTransportError{}, existingCodes)
}

func CheckErrForBan(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}

	if operationErrorCodes, _ := ctx.Value(ctxBanOnOperationError{}).(operationErrorCodesType); xerrors.IsOperationError(
		err, operationErrorCodes...,
	) {
		return true
	}

	if transportErrorCodes, _ := ctx.Value(ctxBanOnTransportError{}).(transportErrorCodesType); xerrors.IsTransportError(
		err, transportErrorCodes...,
	) {
		return true
	}

	return false
}
