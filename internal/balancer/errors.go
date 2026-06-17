package balancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xslices"
)

// Session-create RPC status codes that indicate the target node should be pessimized.
var sessionCreateBanOperationCodes = []Ydb.StatusIds_StatusCode{
	Ydb.StatusIds_OVERLOADED,
	Ydb.StatusIds_UNAVAILABLE,
}

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
		grpcCodes.Canceled,
	}
	badCodes = xslices.Subtract(xslices.Keys(allCodes), goodCodes)
)

type (
	ctxBanOnOperationError          struct{}
	ctxBanOnContextDeadlineExceeded struct{}
	operationErrorCodesType         []Ydb.StatusIds_StatusCode
)

func BanOnOperationError(ctx context.Context, codes ...Ydb.StatusIds_StatusCode) context.Context {
	existingCodes, _ := ctx.Value(ctxBanOnOperationError{}).(operationErrorCodesType)

	allCodes := make(operationErrorCodesType, 0, len(existingCodes)+len(codes))
	allCodes = append(allCodes, existingCodes...)
	allCodes = append(allCodes, codes...)
	allCodes = xslices.Uniq(allCodes)

	return context.WithValue(ctx, ctxBanOnOperationError{}, allCodes)
}

// BanOnContextDeadlineExceeded marks ctx so that context.DeadlineExceeded on the RPC
// pessimizes the connection. Intended for driver-level probes (CreateSession, AttachSession),
// not for query execution timeouts on otherwise healthy nodes.
func BanOnContextDeadlineExceeded(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxBanOnContextDeadlineExceeded{}, true)
}

// BanOnSessionCreate marks ctx for CreateSession/AttachSession RPCs: ban the connection on
// overload, unavailability, or client-side deadline exceeded.
func BanOnSessionCreate(ctx context.Context) context.Context {
	ctx = BanOnOperationError(ctx, sessionCreateBanOperationCodes...)

	return BanOnContextDeadlineExceeded(ctx)
}

func IsBadConn(ctx context.Context, err error, ignoreCodes ...grpcCodes.Code) bool {
	if xerrors.IsTransportError(err, xslices.Subtract(badCodes, ignoreCodes)...) {
		return true
	}

	operationErrorCodes, _ := ctx.Value(ctxBanOnOperationError{}).(operationErrorCodesType)

	if len(operationErrorCodes) > 0 && xerrors.IsOperationError(err, operationErrorCodes...) {
		return true
	}

	banOnContextDeadlineExceeded, _ := ctx.Value(ctxBanOnContextDeadlineExceeded{}).(bool)
	if banOnContextDeadlineExceeded {
		if xerrors.Is(err, context.DeadlineExceeded) {
			return true
		}
	}

	return false
}
