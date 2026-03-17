package conn

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
)

type (
	ctxNoWrappingKey        struct{}
	ctxBanOnOperationError  struct{}
	operationErrorCodesType []Ydb.StatusIds_StatusCode
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
