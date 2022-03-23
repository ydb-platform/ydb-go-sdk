package errors

import (
	"bytes"
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

// StatusCode reports unsuccessful operation status code.
type StatusCode int32

func (e StatusCode) String() string {
	return Ydb.StatusIds_StatusCode_name[int32(e)]
}

// Errors describing unsusccessful operation status.
const (
	StatusUnknownStatus      = StatusCode(Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)
	StatusBadRequest         = StatusCode(Ydb.StatusIds_BAD_REQUEST)
	StatusUnauthorized       = StatusCode(Ydb.StatusIds_UNAUTHORIZED)
	StatusInternalError      = StatusCode(Ydb.StatusIds_INTERNAL_ERROR)
	StatusAborted            = StatusCode(Ydb.StatusIds_ABORTED)
	StatusUnavailable        = StatusCode(Ydb.StatusIds_UNAVAILABLE)
	StatusOverloaded         = StatusCode(Ydb.StatusIds_OVERLOADED)
	StatusSchemeError        = StatusCode(Ydb.StatusIds_SCHEME_ERROR)
	StatusGenericError       = StatusCode(Ydb.StatusIds_GENERIC_ERROR)
	StatusTimeout            = StatusCode(Ydb.StatusIds_TIMEOUT)
	StatusBadSession         = StatusCode(Ydb.StatusIds_BAD_SESSION)
	StatusPreconditionFailed = StatusCode(Ydb.StatusIds_PRECONDITION_FAILED)
	StatusAlreadyExists      = StatusCode(Ydb.StatusIds_ALREADY_EXISTS)
	StatusNotFound           = StatusCode(Ydb.StatusIds_NOT_FOUND)
	StatusSessionExpired     = StatusCode(Ydb.StatusIds_SESSION_EXPIRED)
	StatusCancelled          = StatusCode(Ydb.StatusIds_CANCELLED)
	StatusUndetermined       = StatusCode(Ydb.StatusIds_UNDETERMINED)
	StatusUnsupported        = StatusCode(Ydb.StatusIds_UNSUPPORTED)
	StatusSessionBusy        = StatusCode(Ydb.StatusIds_SESSION_BUSY)
)

func statusCode(s Ydb.StatusIds_StatusCode) StatusCode {
	switch s {
	case Ydb.StatusIds_BAD_REQUEST:
		return StatusBadRequest
	case Ydb.StatusIds_UNAUTHORIZED:
		return StatusUnauthorized
	case Ydb.StatusIds_INTERNAL_ERROR:
		return StatusInternalError
	case Ydb.StatusIds_ABORTED:
		return StatusAborted
	case Ydb.StatusIds_UNAVAILABLE:
		return StatusUnavailable
	case Ydb.StatusIds_OVERLOADED:
		return StatusOverloaded
	case Ydb.StatusIds_SCHEME_ERROR:
		return StatusSchemeError
	case Ydb.StatusIds_GENERIC_ERROR:
		return StatusGenericError
	case Ydb.StatusIds_TIMEOUT:
		return StatusTimeout
	case Ydb.StatusIds_BAD_SESSION:
		return StatusBadSession
	case Ydb.StatusIds_PRECONDITION_FAILED:
		return StatusPreconditionFailed
	case Ydb.StatusIds_ALREADY_EXISTS:
		return StatusAlreadyExists
	case Ydb.StatusIds_NOT_FOUND:
		return StatusNotFound
	case Ydb.StatusIds_SESSION_EXPIRED:
		return StatusSessionExpired
	case Ydb.StatusIds_CANCELLED:
		return StatusCancelled
	case Ydb.StatusIds_UNDETERMINED:
		return StatusUndetermined
	case Ydb.StatusIds_UNSUPPORTED:
		return StatusUnsupported
	case Ydb.StatusIds_SESSION_BUSY:
		return StatusSessionBusy
	default:
		return StatusUnknownStatus
	}
}

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
