package errors

import (
	"bytes"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// operationError reports about operation fail.
type operationError struct {
	code   Ydb.StatusIds_StatusCode
	issues []*Ydb_Issue.IssueMessage
}

func (e *operationError) isYdbError() {}

func (e *operationError) Code() int32 {
	return int32(e.code)
}

func (e *operationError) Name() string {
	return e.code.String()
}

type operation interface {
	GetStatus() Ydb.StatusIds_StatusCode
	GetIssues() []*Ydb_Issue.IssueMessage
}

// WithIssues is an option for construct operation error with issues list
// WithIssues must use as `NewOpError(WithIssues(issues))`
func WithIssues(issues []*Ydb_Issue.IssueMessage) oeOpt {
	return func(oe *operationError) {
		oe.issues = issues
	}
}

// WithStatusCode is an option for construct operation error with reason code
// WithStatusCode must use as `NewOpError(WithStatusCode(reason))`
func WithStatusCode(code Ydb.StatusIds_StatusCode) oeOpt {
	return func(oe *operationError) {
		oe.code = code
	}
}

// FromOperation is an option for construct operation error from operation
// FromOperation must use as `NewOpError(FromOperation(operation))`
func FromOperation(operation operation) oeOpt {
	return func(oe *operationError) {
		oe.code = operation.GetStatus()
		oe.issues = operation.GetIssues()
	}
}

type oeOpt func(ops *operationError)

func NewOpError(opts ...oeOpt) error {
	oe := &operationError{
		code: Ydb.StatusIds_STATUS_CODE_UNSPECIFIED,
	}
	for _, f := range opts {
		f(oe)
	}
	return oe
}

func (e *operationError) Issues() []*Ydb_Issue.IssueMessage {
	return e.issues
}

func (e *operationError) Error() string {
	var buf bytes.Buffer
	buf.WriteString("operation error: ")
	buf.WriteString(e.code.String())
	if len(e.issues) > 0 {
		buf.WriteByte(':')
		dumpIssues(&buf, e.issues)
	}
	return buf.String()
}

// IsOperationError reports whether err is operationError with given status codes.
func IsOperationError(err error, codes ...Ydb.StatusIds_StatusCode) bool {
	var op *operationError
	if !errors.As(err, &op) {
		return false
	}
	if len(codes) == 0 {
		return true
	}
	for _, code := range codes {
		if op.code == code {
			return true
		}
	}
	return false
}

func (e *operationError) OperationStatus() OperationStatus {
	switch e.code {
	case
		Ydb.StatusIds_ABORTED,
		Ydb.StatusIds_UNAVAILABLE,
		Ydb.StatusIds_OVERLOADED,
		Ydb.StatusIds_BAD_SESSION,
		Ydb.StatusIds_SESSION_BUSY:
		return OperationNotFinished
	case
		Ydb.StatusIds_UNDETERMINED:
		return OperationStatusUndefined
	default:
		return OperationFinished
	}
}

func (e *operationError) BackoffType() BackoffType {
	switch e.code {
	case
		Ydb.StatusIds_OVERLOADED:
		return BackoffTypeSlowBackoff
	case
		Ydb.StatusIds_ABORTED,
		Ydb.StatusIds_UNAVAILABLE,
		Ydb.StatusIds_CANCELLED,
		Ydb.StatusIds_SESSION_BUSY,
		Ydb.StatusIds_UNDETERMINED:
		return BackoffTypeFastBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (e *operationError) MustDeleteSession() bool {
	switch e.code {
	case
		Ydb.StatusIds_BAD_SESSION,
		Ydb.StatusIds_SESSION_EXPIRED,
		Ydb.StatusIds_SESSION_BUSY:
		return true
	default:
		return false
	}
}

func OperationError(err error) Error {
	var o *operationError
	if errors.As(err, &o) {
		return o
	}
	return nil
}
