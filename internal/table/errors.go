package table

import (
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errNilClient = xerrors.Wrap(errors.New("table client is not initialized"))

	// errClosedClient returned by a Client instance to indicate
	// that Client is closed early and not able to complete requested operation.
	errClosedClient = xerrors.Wrap(errors.New("table client closed early"))

	// errSessionPoolOverflow returned by a Client instance to indicate
	// that the Client is full and requested operation is not able to complete.
	errSessionPoolOverflow = xerrors.Wrap(errors.New("session pool overflow"))

	// errSessionUnderShutdown returned by a Client instance to indicate that
	// requested session is under shutdown.
	errSessionUnderShutdown = xerrors.Wrap(errors.New("session under shutdown"))

	// errSessionClosed returned by a Client instance to indicate that
	// requested session is closed early.
	errSessionClosed = xerrors.Wrap(errors.New("session closed early"))

	// errNoProgress returned by a Client instance to indicate that
	// operation could not be completed.
	errNoProgress = xerrors.Wrap(errors.New("no progress"))

	// errNodeIsNotObservable returned by a Client instance to indicate that required node is not observable
	errNodeIsNotObservable = xerrors.Wrap(errors.New("node is not observable"))

	// errParamsRequired returned by a Client instance to indicate that required params is not defined
	errParamsRequired = xerrors.Wrap(errors.New("params required"))
)

func isCreateSessionErrorRetriable(err error) bool {
	switch {
	case
		xerrors.Is(err, errSessionPoolOverflow),
		xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED),
		xerrors.IsTransportError(
			err,
			grpcCodes.ResourceExhausted,
			grpcCodes.DeadlineExceeded,
			grpcCodes.Unavailable,
		):
		return true
	default:
		return false
	}
}
