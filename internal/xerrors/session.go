package xerrors

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"
)

func MustDeleteTableOrQuerySession(err error) bool {
	// Context errors (context.Canceled, context.DeadlineExceeded) indicate that a
	// query may still be running on the server side. The session must be invalidated
	// to prevent SESSION_BUSY errors on subsequent requests.
	if IsContextError(err) {
		return true
	}

	if IsOperationError(err,
		Ydb.StatusIds_BAD_SESSION,
		Ydb.StatusIds_SESSION_BUSY,
		Ydb.StatusIds_SESSION_EXPIRED,
	) {
		return true
	}

	if IsTransportError(err,
		grpcCodes.Canceled,
		grpcCodes.Unknown,
		grpcCodes.InvalidArgument,
		grpcCodes.DeadlineExceeded,
		grpcCodes.NotFound,
		grpcCodes.AlreadyExists,
		grpcCodes.PermissionDenied,
		grpcCodes.FailedPrecondition,
		grpcCodes.Aborted,
		grpcCodes.Unimplemented,
		grpcCodes.Internal,
		grpcCodes.Unavailable,
		grpcCodes.DataLoss,
		grpcCodes.Unauthenticated,
	) {
		return true
	}

	return false
}
