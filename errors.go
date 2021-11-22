package ydb

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
)

func IterateByIssues(err error, it func(message string, code uint32, severity uint32)) {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return
	}
	issues := o.Issues()
	iterate(issues, it)
}

func iterate(issues errors.IssueIterator, it func(message string, code uint32, severity uint32)) {
	l := issues.Len()
	for i := 0; i < l; i++ {
		issue, nested := issues.Get(i)
		it(issue.Message, issue.Code, issue.Severity)
		iterate(nested, it)
	}
}

func IsTimeoutError(err error) bool {
	return errors.IsTimeoutError(err)
}

func IsTransportError(err error) bool {
	return TransportErrorDescription(err) != nil
}

func IsTransportErrorCancelled(err error) bool {
	d := TransportErrorDescription(err)
	return d != nil && d.Code() == int32(errors.TransportErrorCanceled)
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

func OperationErrorDescription(err error) Error {
	var o *errors.OpError
	if errors.As(err, &o) {
		return o
	}
	return nil
}

func IsStatusAlreadyExistsError(err error) bool {
	d := OperationErrorDescription(err)
	return d != nil && d.Code() == int32(errors.StatusAlreadyExists)
}

func IsStatusNotFoundError(err error) bool {
	d := OperationErrorDescription(err)
	return d != nil && d.Code() == int32(errors.StatusNotFound)
}

func IsStatusSchemeError(err error) bool {
	d := OperationErrorDescription(err)
	return d != nil && d.Code() == int32(errors.StatusSchemeError)
}
