package ydb

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	grpccodes "google.golang.org/grpc/codes"

	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb"
	"github.com/yandex-cloud/ydb-go-sdk/internal/api/protos/Ydb_Issue"
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
	if xs := x.IssueMessage; len(xs) > 0 {
		nested = IssueIterator(xs)
	}
	return Issue{
		Message:  *x.Message,
		Code:     *x.IssueCode,
		Severity: *x.Severity,
	}, nested
}

type TransportError struct {
	Reason TransportErrorCode

	message string
}

func (t *TransportError) Error() string {
	s := "transport error: " + t.Reason.String()
	if t.message != "" {
		s += ": " + t.message
	}
	return s
}

// IsTransportError reports whether err is TransportError with given code as
// the Reason.
func IsTransportError(err error, code TransportErrorCode) bool {
	t, ok := err.(*TransportError)
	if !ok {
		return false
	}
	return t.Reason == code
}

// OpError reports about operation fail.
type OpError struct {
	Reason StatusCode

	issues []*Ydb_Issue.IssueMessage
}

func (e *OpError) Issues() IssueIterator {
	return IssueIterator(e.issues)
}

func (e *OpError) Error() string {
	if len(e.issues) == 0 {
		return e.Reason.String()
	}
	var buf bytes.Buffer
	buf.WriteString("rpc call error: ")
	buf.WriteString(e.Reason.String())
	if len(e.issues) > 0 {
		buf.WriteByte(':')
		dumpIssues(&buf, e.issues)
	}
	return buf.String()
}

// IsOpError reports whether err is OpError with given code as the Reason.
func IsOpError(err error, code StatusCode) bool {
	op, ok := err.(*OpError)
	if !ok {
		return false
	}
	return op.Reason == code
}

func iterateIssues(issues []*Ydb_Issue.IssueMessage, it func(Issue)) {
	for _, x := range issues {
		it(Issue{
			Message:  *x.Message,
			Code:     *x.IssueCode,
			Severity: *x.Severity,
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
		if code := *m.IssueCode; code != 0 {
			buf.WriteByte('#')
			buf.WriteString(strconv.Itoa(int(code)))
			buf.WriteByte(' ')
		}
		buf.WriteString(strings.TrimSuffix(*m.Message, "."))
		dumpIssues(buf, m.IssueMessage)
		buf.WriteByte('}')
	}
}

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
	default:
		return StatusUnknownStatus
	}
}

type TransportErrorCode int32

func (t TransportErrorCode) String() string {
	return transportErrorString(t)
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
	grpccodes.Canceled:           TransportErrorCanceled,
	grpccodes.Unknown:            TransportErrorUnknown,
	grpccodes.InvalidArgument:    TransportErrorInvalidArgument,
	grpccodes.DeadlineExceeded:   TransportErrorDeadlineExceeded,
	grpccodes.NotFound:           TransportErrorNotFound,
	grpccodes.AlreadyExists:      TransportErrorAlreadyExists,
	grpccodes.PermissionDenied:   TransportErrorPermissionDenied,
	grpccodes.ResourceExhausted:  TransportErrorResourceExhausted,
	grpccodes.FailedPrecondition: TransportErrorFailedPrecondition,
	grpccodes.Aborted:            TransportErrorAborted,
	grpccodes.OutOfRange:         TransportErrorOutOfRange,
	grpccodes.Unimplemented:      TransportErrorUnimplemented,
	grpccodes.Internal:           TransportErrorInternal,
	grpccodes.Unavailable:        TransportErrorUnavailable,
	grpccodes.DataLoss:           TransportErrorDataLoss,
	grpccodes.Unauthenticated:    TransportErrorUnauthenticated,
}

func transportErrorCode(c grpccodes.Code) TransportErrorCode {
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
