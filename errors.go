package ydb

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	ratelimiterErrors "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

func IterateByIssues(err error, it func(message string, code uint32, severity uint32)) {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return
	}
	issues := o.Issues()
	iterate(issues, it)
}

func iterate(issues []*Ydb_Issue.IssueMessage, it func(message string, code uint32, severity uint32)) {
	for _, issue := range issues {
		it(issue.GetMessage(), issue.GetIssueCode(), issue.GetSeverity())
		iterate(issue.GetIssues(), it)
	}
}

func IsTimeoutError(err error) bool {
	return errors.IsTimeoutError(err)
}

func IsTransportError(err error) bool {
	return TransportErrorDescription(err) != nil
}

func IsTransportErrorCode(err error, codes ...int32) bool {
	d := TransportErrorDescription(err)
	if d == nil {
		return false
	}
	for _, code := range codes {
		if d.Code() == code {
			return true
		}
	}
	return false
}

func IsTransportErrorCancelled(err error) bool {
	return IsTransportErrorCode(err, int32(errors.TransportErrorCanceled))
}

func IsTransportErrorResourceExhausted(err error) bool {
	return IsTransportErrorCode(err, int32(errors.TransportErrorResourceExhausted))
}

type Error interface {
	error

	Code() int32
	Name() string
}

func TransportErrorDescription(err error) Error {
	var t *errors.TransportError
	if errors.As(err, &t) {
		return t
	}
	return nil
}

func IsYdbError(err error) bool {
	return IsTransportError(err) || IsOperationError(err)
}

func IsOperationError(err error) bool {
	return OperationErrorDescription(err) != nil
}

func IsOperationErrorCode(err error, codes ...int32) bool {
	d := OperationErrorDescription(err)
	if d == nil {
		return false
	}
	for _, code := range codes {
		if d.Code() == code {
			return true
		}
	}
	return false
}

func OperationErrorDescription(err error) Error {
	var o *errors.OpError
	if errors.As(err, &o) {
		return o
	}
	return nil
}

func IsOperationErrorOverloaded(err error) bool {
	return IsOperationErrorCode(err, int32(errors.StatusOverloaded))
}

func IsOperationErrorUnavailable(err error) bool {
	return IsOperationErrorCode(err, int32(errors.StatusUnavailable))
}

func IsOperationErrorAlreadyExistsError(err error) bool {
	return IsOperationErrorCode(err, int32(errors.StatusAlreadyExists))
}

func IsOperationErrorNotFoundError(err error) bool {
	return IsOperationErrorCode(err, int32(errors.StatusNotFound))
}

func IsOperationErrorSchemeError(err error) bool {
	return IsOperationErrorCode(err, int32(errors.StatusSchemeError))
}

func IsRatelimiterAcquireError(err error) bool {
	return ratelimiterErrors.IsAcquireError(err)
}

func ToRatelimiterAcquireError(err error) ratelimiter.AcquireError {
	return ratelimiterErrors.ToAcquireError(err)
}
