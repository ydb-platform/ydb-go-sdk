package ydb

import (
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	ratelimiterErrors "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

func IterateByIssues(err error, it func(message string, code Ydb.StatusIds_StatusCode, severity uint32)) {
	xerrors.IterateByIssues(err, it)
}

func IsTimeoutError(err error) bool {
	return xerrors.IsTimeoutError(err)
}

func IsTransportError(err error, codes ...grpcCodes.Code) bool {
	return xerrors.IsTransportError(err, codes...)
}

type Error xerrors.Error

func TransportError(err error) Error {
	return xerrors.TransportError(err)
}

func IsYdbError(err error) bool {
	return xerrors.IsYdb(err)
}

func IsOperationError(err error, codes ...Ydb.StatusIds_StatusCode) bool {
	return xerrors.IsOperationError(err, codes...)
}

func OperationError(err error) Error {
	return xerrors.OperationError(err)
}

func IsOperationErrorOverloaded(err error) bool {
	return IsOperationError(err, Ydb.StatusIds_OVERLOADED)
}

func IsOperationErrorUnavailable(err error) bool {
	return IsOperationError(err, Ydb.StatusIds_UNAVAILABLE)
}

func IsOperationErrorAlreadyExistsError(err error) bool {
	return IsOperationError(err, Ydb.StatusIds_ALREADY_EXISTS)
}

func IsOperationErrorNotFoundError(err error) bool {
	return IsOperationError(err, Ydb.StatusIds_NOT_FOUND)
}

func IsOperationErrorSchemeError(err error) bool {
	return IsOperationError(err, Ydb.StatusIds_SCHEME_ERROR)
}

func IsRatelimiterAcquireError(err error) bool {
	return ratelimiterErrors.IsAcquireError(err)
}

func ToRatelimiterAcquireError(err error) ratelimiter.AcquireError {
	return ratelimiterErrors.ToAcquireError(err)
}
