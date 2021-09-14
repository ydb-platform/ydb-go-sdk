package internal

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	grpc "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

type Issue struct {
	Message  string
	Code     uint32
	Severity uint32
}

var ErrOperationNotReady = errors.New("operation is not ready yet")

type IssueIterator []*Ydb_Issue.IssueMessage

func (it IssueIterator) Len() int {
	return len(it)
}

func (it IssueIterator) Get(i int) (issue Issue, nested IssueIterator) {
	x := it[i]
	if xs := x.Issues; len(xs) > 0 {
		nested = IssueIterator(xs)
	}
	return Issue{
		Message:  x.GetMessage(),
		Code:     x.GetIssueCode(),
		Severity: x.GetSeverity(),
	}, nested
}

type TransportError struct {
	Reason TransportErrorCode

	message string
	err     error
}

func NewTransportError(reason grpc.Code, message string, err error) *TransportError {
	return &TransportError{
		Reason:  TransportErrorCode(reason),
		message: message,
		err:     err,
	}
}

func (t *TransportError) Error() string {
	s := "ydb: transport error: " + t.Reason.String()
	if t.message != "" {
		s += ": " + t.message
	}
	return s
}

func (t *TransportError) Unwrap() error {
	return t.err
}

// OpError reports about operation fail.
type OpError struct {
	Reason StatusCode

	issues []*Ydb_Issue.IssueMessage
}

func NewOpError(reason Ydb.StatusIds_StatusCode, issues []*Ydb_Issue.IssueMessage) *OpError {
	return &OpError{
		Reason: StatusCode(reason),
		issues: issues,
	}
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

func iterateIssues(issues []*Ydb_Issue.IssueMessage, it func(Issue)) {
	for _, x := range issues {
		it(Issue{
			Message:  x.GetMessage(),
			Code:     x.GetIssueCode(),
			Severity: x.GetSeverity(),
		})
	}
}

func dumpIssues(buf *bytes.Buffer, ms []*Ydb_Issue.IssueMessage) {
	if len(ms) == 0 {
		return
	}
	buf.WriteByte(' ')
	buf.WriteByte('[')
	defer buf.WriteByte(']')
	for _, m := range ms {
		buf.WriteByte('{')
		if code := m.GetIssueCode(); code != 0 {
			buf.WriteByte('#')
			buf.WriteString(strconv.Itoa(int(code)))
			buf.WriteByte(' ')
		}
		buf.WriteString(strings.TrimSuffix(m.GetMessage(), "."))
		dumpIssues(buf, m.Issues)
		buf.WriteByte('}')
	}
}

// StatusCode reports unsuccessful operation status code.
type StatusCode int32

func (e StatusCode) String() string {
	return Ydb.StatusIds_StatusCode_name[int32(e)]
}

func (e StatusCode) RetryType() RetryType {
	switch e {
	case
		StatusAborted,
		StatusUnavailable,
		StatusOverloaded,
		StatusBadSession,
		StatusSessionBusy,
		StatusNotFound:
		return RetryTypeAny
	case
		StatusCancelled,
		StatusUndetermined:
		return RetryTypeIdempotent
	default:
		return RetryTypeNoRetry
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
		StatusBadSession,
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

type TransportErrorCode int32

func (t TransportErrorCode) String() string {
	return transportErrorString(t)
}

func (t TransportErrorCode) RetryType() RetryType {
	switch t {
	case
		TransportErrorResourceExhausted,
		TransportErrorAborted:
		return RetryTypeAny
	case
		TransportErrorInternal,
		TransportErrorUnavailable:
		return RetryTypeIdempotent
	default:
		return RetryTypeNoRetry
	}
}

func (t TransportErrorCode) BackoffType() BackoffType {
	switch t {
	case
		TransportErrorInternal,
		TransportErrorUnavailable:
		return BackoffTypeFastBackoff
	case
		TransportErrorResourceExhausted:
		return BackoffTypeSlowBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (t TransportErrorCode) MustDeleteSession() bool {
	switch t {
	case
		TransportErrorCanceled,
		TransportErrorResourceExhausted,
		TransportErrorOutOfRange:
		return false
	default:
		return true
	}
}

const (
	TransportErrorUnknownCode TransportErrorCode = iota
	TransportErrorCanceled
	TransportErrorUnknown
	TransportErrorInvalidArgument
	TransportErrorDeadlineExceeded
	TransportErrorNotFound
	TransportErrorAlreadyExists
	TransportErrorPermissionDenied
	TransportErrorResourceExhausted
	TransportErrorFailedPrecondition
	TransportErrorAborted
	TransportErrorOutOfRange
	TransportErrorUnimplemented
	TransportErrorInternal
	TransportErrorUnavailable
	TransportErrorDataLoss
	TransportErrorUnauthenticated
)

var grpcCodesToTransportError = [...]TransportErrorCode{
	grpc.Canceled:           TransportErrorCanceled,
	grpc.Unknown:            TransportErrorUnknown,
	grpc.InvalidArgument:    TransportErrorInvalidArgument,
	grpc.DeadlineExceeded:   TransportErrorDeadlineExceeded,
	grpc.NotFound:           TransportErrorNotFound,
	grpc.AlreadyExists:      TransportErrorAlreadyExists,
	grpc.PermissionDenied:   TransportErrorPermissionDenied,
	grpc.ResourceExhausted:  TransportErrorResourceExhausted,
	grpc.FailedPrecondition: TransportErrorFailedPrecondition,
	grpc.Aborted:            TransportErrorAborted,
	grpc.OutOfRange:         TransportErrorOutOfRange,
	grpc.Unimplemented:      TransportErrorUnimplemented,
	grpc.Internal:           TransportErrorInternal,
	grpc.Unavailable:        TransportErrorUnavailable,
	grpc.DataLoss:           TransportErrorDataLoss,
	grpc.Unauthenticated:    TransportErrorUnauthenticated,
}

func transportErrorCode(c grpc.Code) TransportErrorCode {
	if int(c) < len(grpcCodesToTransportError) {
		return grpcCodesToTransportError[c]
	}
	return TransportErrorUnknownCode
}

var transportErrorToString = [...]string{
	TransportErrorCanceled:           "canceled",
	TransportErrorUnknown:            "unknown",
	TransportErrorInvalidArgument:    "invalid argument",
	TransportErrorDeadlineExceeded:   "deadline exceeded",
	TransportErrorNotFound:           "not found",
	TransportErrorAlreadyExists:      "already exists",
	TransportErrorPermissionDenied:   "permission denied",
	TransportErrorResourceExhausted:  "resource exhausted",
	TransportErrorFailedPrecondition: "failed precondition",
	TransportErrorAborted:            "aborted",
	TransportErrorOutOfRange:         "out of range",
	TransportErrorUnimplemented:      "unimplemented",
	TransportErrorInternal:           "internal",
	TransportErrorUnavailable:        "unavailable",
	TransportErrorDataLoss:           "data loss",
	TransportErrorUnauthenticated:    "unauthenticated",
}

func transportErrorString(t TransportErrorCode) string {
	if int(t) < len(transportErrorToString) {
		return transportErrorToString[t]
	}
	return "unknown code"
}
