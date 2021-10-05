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

func IsTransportError(err error) (ok bool, code int32, name string) {
	var t *errors.TransportError
	if !errors.As(err, &t) {
		return
	}
	return true, int32(t.Reason), t.Reason.String()
}

func IsOperationError(err error) (ok bool, code int32, name string) {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return
	}
	return true, int32(o.Reason), o.Reason.String()
}

func IsStatusAlreadyExistsError(err error) bool {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return false
	}
	return o.Reason == errors.StatusAlreadyExists
}

func IsStatusNotFoundError(err error) bool {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return false
	}
	return o.Reason == errors.StatusNotFound
}

func IsStatusSchemeError(err error) bool {
	var o *errors.OpError
	if !errors.As(err, &o) {
		return false
	}
	return o.Reason == errors.StatusSchemeError
}
