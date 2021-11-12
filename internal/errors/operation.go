package errors

import (
	"bytes"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// OpError reports about operation fail.
type OpError struct {
	Reason StatusCode

	issues []*Ydb_Issue.IssueMessage
}

type operation interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

// WithOEIssues is an option for construct operation error with issues list
// WithOEIssues must use as `NewOpError(WithOEIssues(issues))`
func WithOEIssues(issues []*Ydb_Issue.IssueMessage) oeOpt {
	return func(oe *OpError) {
		oe.issues = issues
	}
}

// WithOEReason is an option for construct operation error with reason code
// WithOEReason must use as `NewOpError(WithOEReason(reason))`
func WithOEReason(reason StatusCode) oeOpt {
	return func(oe *OpError) {
		oe.Reason = reason
	}
}

// WithOEOperation is an option for construct operation error from operation
// WithOEOperation must use as `NewOpError(WithOEOperation(operation))`
func WithOEOperation(operation operation) oeOpt {
	return func(oe *OpError) {
		oe.Reason = statusCode(operation.GetStatus())
		oe.issues = operation.GetIssues()
	}
}

type oeOpt func(ops *OpError)

func NewOpError(opts ...oeOpt) error {
	oe := &OpError{
		Reason: StatusUnknownStatus,
	}
	for _, f := range opts {
		f(oe)
	}
	return oe
}

func (e *OpError) Issues() IssueIterator {
	return IssueIterator(e.issues)
}

func (e *OpError) Error() string {
	if len(e.issues) == 0 {
		return e.Reason.String()
	}
	var buf bytes.Buffer
	buf.WriteString("ydb: operation error: ")
	buf.WriteString(e.Reason.String())
	if len(e.issues) > 0 {
		buf.WriteByte(':')
		dumpIssues(&buf, e.issues)
	}
	return buf.String()
}

// nolint:unused, deadcode
func iterateIssues(issues []*Ydb_Issue.IssueMessage, it func(Issue)) {
	for _, x := range issues {
		it(Issue{
			Message:  x.GetMessage(),
			Code:     x.GetIssueCode(),
			Severity: x.GetSeverity(),
		})
	}
}

// IsOpError reports whether err is OpError with given code as the Reason.
func IsOpError(err error, code StatusCode) bool {
	var op *OpError
	if !errors.As(err, &op) {
		return false
	}
	return op.Reason == code
}

func (e StatusCode) OperationCompleted() OperationCompleted {
	switch e {
	case
		StatusAborted,
		StatusUnavailable,
		StatusOverloaded,
		StatusBadSession,
		StatusSessionBusy:
		return OperationCompletedFalse
	case
		StatusUndetermined:
		return OperationCompletedUndefined
	default:
		return OperationCompletedTrue
	}
}

func (e StatusCode) BackoffType() BackoffType {
	switch e {
	case
		StatusOverloaded:
		return BackoffTypeSlowBackoff
	case
		StatusAborted,
		StatusUnavailable,
		StatusCancelled,
		StatusSessionBusy,
		StatusUndetermined:
		return BackoffTypeFastBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (e StatusCode) MustDeleteSession() bool {
	switch e {
	case
		StatusBadSession,
		StatusSessionExpired,
		StatusSessionBusy:
		return true
	default:
		return false
	}
}
