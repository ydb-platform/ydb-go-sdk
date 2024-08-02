package conn

import (
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func IsBadConn(err error, goodConnCodes ...grpcCodes.Code) bool {
	if !xerrors.IsTransportError(err) {
		return false
	}

	if xerrors.IsTransportError(err,
		append(
			goodConnCodes,
			grpcCodes.ResourceExhausted,
			grpcCodes.OutOfRange,
			grpcCodes.OK,
			// grpcCodes.Canceled,
			// grpcCodes.Unknown,
			// grpcCodes.InvalidArgument,
			// grpcCodes.DeadlineExceeded,
			// grpcCodes.NotFound,
			// grpcCodes.AlreadyExists,
			// grpcCodes.PermissionDenied,
			// grpcCodes.FailedPrecondition,
			// grpcCodes.Aborted,
			// grpcCodes.OutOfRange,
			// grpcCodes.Unimplemented,
			// grpcCodes.Internal,
			// grpcCodes.DataLoss,
			// grpcCodes.Unauthenticated,
		)...,
	) {
		return false
	}

	return true
}

type grpcError struct {
	err error

	nodeID  uint32
	address string
}

func (e *grpcError) Error() string {
	return e.err.Error()
}

func (e *grpcError) As(target any) bool {
	return xerrors.As(e.err, target)
}

func (e *grpcError) NodeID() uint32 {
	return e.nodeID
}

func (e *grpcError) Address() string {
	return e.address
}

func withConnInfo(err error, nodeID uint32, address string) error {
	return &grpcError{
		err:     err,
		nodeID:  nodeID,
		address: address,
	}
}
