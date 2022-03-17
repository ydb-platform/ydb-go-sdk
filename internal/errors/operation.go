package errors

import (
	"bytes"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// OperationError reports about operation fail.
type OperationError struct {
	Reason StatusCode

	issues []*Ydb_Issue.IssueMessage
}

func (e *OperationError) isYdbError() {}

func (e *OperationError) Code() int32 {
	return int32(e.Reason)
}

func (e *OperationError) Name() string {
	return e.Reason.String()
}

type operation interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

// WithOEIssues is an option for construct operation error with issues list
// WithOEIssues must use as `NewOpError(WithOEIssues(issues))`
func WithOEIssues(issues []*Ydb_Issue.IssueMessage) oeOpt {
	return func(oe *OperationError) {
		oe.issues = issues
	}
}

// WithOEReason is an option for construct operation error with reason code
// WithOEReason must use as `NewOpError(WithOEReason(reason))`
func WithOEReason(reason StatusCode) oeOpt {
	return func(oe *OperationError) {
		oe.Reason = reason
	}
}

// WithOEOperation is an option for construct operation error from operation
// WithOEOperation must use as `NewOpError(WithOEOperation(operation))`
func WithOEOperation(operation operation) oeOpt {
	return func(oe *OperationError) {
		oe.Reason = statusCode(operation.GetStatus())
		oe.issues = operation.GetIssues()
	}
}

type oeOpt func(ops *OperationError)

func NewOpError(opts ...oeOpt) error {
	oe := &OperationError{
		Reason: StatusUnknownStatus,
	}
	for _, f := range opts {
		f(oe)
	}
	return oe
}

func (e *OperationError) Issues() []*Ydb_Issue.IssueMessage {
	return e.issues
}

func (e *OperationError) Error() string {
	if len(e.issues) == 0 {
		return e.Reason.String()
	}
	var buf bytes.Buffer
	buf.WriteString("operation error: ")
	buf.WriteString(e.Reason.String())
	if len(e.issues) > 0 {
		buf.WriteByte(':')
		dumpIssues(&buf, e.issues)
	}
	return buf.String()
}

// IsOpError reports whether err is OperationError with given code as the Reason.
func IsOpError(err error, codes ...StatusCode) bool {
	var op *OperationError
	if !errors.As(err, &op) {
		return false
	}
	if len(codes) == 0 {
		return true
	}
	for _, code := range codes {
		if op.Reason == code {
			return true
		}
	}
	return false
}

func (e StatusCode) OperationStatus() OperationStatus {
	switch e {
	case
		StatusAborted,
		StatusUnavailable,
		StatusOverloaded,
		StatusBadSession,
		StatusSessionBusy:
		return OperationNotFinished
	case
		StatusUndetermined:
		return OperationStatusUndefined
	default:
		return OperationFinished
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
