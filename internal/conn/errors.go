package conn

import (
	"context"

	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
)

var (
	allCodes = map[grpcCodes.Code]struct{}{
		grpcCodes.OK:                 {},
		grpcCodes.Canceled:           {},
		grpcCodes.Unknown:            {},
		grpcCodes.InvalidArgument:    {},
		grpcCodes.DeadlineExceeded:   {},
		grpcCodes.NotFound:           {},
		grpcCodes.AlreadyExists:      {},
		grpcCodes.PermissionDenied:   {},
		grpcCodes.ResourceExhausted:  {},
		grpcCodes.FailedPrecondition: {},
		grpcCodes.Aborted:            {},
		grpcCodes.OutOfRange:         {},
		grpcCodes.Unimplemented:      {},
		grpcCodes.Internal:           {},
		grpcCodes.Unavailable:        {},
		grpcCodes.DataLoss:           {},
		grpcCodes.Unauthenticated:    {},
	}
	goodCodes = []grpcCodes.Code{
		grpcCodes.ResourceExhausted,
		grpcCodes.OutOfRange,
		grpcCodes.OK,
	}
	badCodes = xslices.Subtract(xslices.Keys(allCodes), goodCodes)
)

func IsBadConn(ctx context.Context, err error, ignoreCodes ...grpcCodes.Code) bool {
	if xerrors.IsTransportError(err, xslices.Subtract(badCodes, ignoreCodes)...) {
		return true
	}

	operationErrorCodes, _ := ctx.Value(ctxBanOnOperationError{}).(operationErrorCodesType)

	if len(operationErrorCodes) > 0 && xerrors.IsOperationError(err, operationErrorCodes...) {
		return true
	}

	return false
}
