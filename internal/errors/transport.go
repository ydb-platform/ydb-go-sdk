package errors

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

type TransportError struct {
	Reason TransportErrorCode

	message string
	err     error
	details []interface{}
	address string
}

func (e *TransportError) isYdbError() {}

func (e *TransportError) Code() int32 {
	return int32(e.Reason)
}

func (e *TransportError) Name() string {
	return e.Reason.String()
}

type teOpt func(te *TransportError)

func WithTEReason(reason TransportErrorCode) teOpt {
	return func(te *TransportError) {
		te.Reason = reason
	}
}

func WithTEAddress(address string) teOpt {
	return func(te *TransportError) {
		te.address = address
	}
}

// WithTEError stores err into transport error
func WithTEError(err error) teOpt {
	return func(te *TransportError) {
		te.err = err
	}
}

// WithTEMessage stores message into transport error
func WithTEMessage(message string) teOpt {
	return func(te *TransportError) {
		te.message = message
	}
}

// WithTEOperation stores reason code into transport error from operation
func WithTEOperation(operation operation) teOpt {
	return func(te *TransportError) {
		te.Reason = TransportErrorCode(operation.GetStatus())
	}
}

// NewTransportError returns a new transport error with given options
func NewTransportError(opts ...teOpt) error {
	te := &TransportError{
		Reason: TransportErrorUnknownCode,
	}
	for _, f := range opts {
		f(te)
	}
	return WithStackTrace(fmt.Errorf("%w", te), WithSkipDepth(1))
}

func (e *TransportError) Error() string {
	var b bytes.Buffer
	b.WriteString("transport error: ")
	b.WriteString(e.Reason.String())
	if e.message != "" {
		b.WriteString(", message: ")
		b.WriteString(e.message)
	}
	if len(e.address) > 0 {
		b.WriteString(", address: ")
		b.WriteString(e.address)
	}
	if len(e.details) > 0 {
		b.WriteString(", details: ")
		if len(e.details) > 0 {
			b.WriteString(", details:")
			for _, detail := range e.details {
				b.WriteString(fmt.Sprintf("\n- %v", detail))
			}
		}
	}
	return b.String()
}

func (e *TransportError) Unwrap() error {
	return e.err
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

type TransportErrorCode int32

func (t TransportErrorCode) String() string {
	return transportErrorString(t)
}

func (t TransportErrorCode) OperationStatus() OperationStatus {
	switch t {
	case
		TransportErrorAborted,
		TransportErrorResourceExhausted:
		return OperationNotFinished
	case
		TransportErrorInternal,
		TransportErrorCanceled,
		TransportErrorUnavailable:
		return OperationStatusUndefined
	default:
		return OperationFinished
	}
}

func (t TransportErrorCode) BackoffType() BackoffType {
	switch t {
	case
		TransportErrorInternal,
		TransportErrorCanceled,
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
	grpcCodes.Canceled:           TransportErrorCanceled,
	grpcCodes.Unknown:            TransportErrorUnknown,
	grpcCodes.InvalidArgument:    TransportErrorInvalidArgument,
	grpcCodes.DeadlineExceeded:   TransportErrorDeadlineExceeded,
	grpcCodes.NotFound:           TransportErrorNotFound,
	grpcCodes.AlreadyExists:      TransportErrorAlreadyExists,
	grpcCodes.PermissionDenied:   TransportErrorPermissionDenied,
	grpcCodes.ResourceExhausted:  TransportErrorResourceExhausted,
	grpcCodes.FailedPrecondition: TransportErrorFailedPrecondition,
	grpcCodes.Aborted:            TransportErrorAborted,
	grpcCodes.OutOfRange:         TransportErrorOutOfRange,
	grpcCodes.Unimplemented:      TransportErrorUnimplemented,
	grpcCodes.Internal:           TransportErrorInternal,
	grpcCodes.Unavailable:        TransportErrorUnavailable,
	grpcCodes.DataLoss:           TransportErrorDataLoss,
	grpcCodes.Unauthenticated:    TransportErrorUnauthenticated,
}

func FromGRPCCode(c grpcCodes.Code) TransportErrorCode {
	if int(c) < len(grpcCodesToTransportError) {
		return grpcCodesToTransportError[c]
	}
	return TransportErrorUnknownCode
}

var transportErrorToString = [...]string{
	TransportErrorUnknownCode:        "unknown code",
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

// IsTransportError reports whether err is TransportError with given code as
// the Reason.
func IsTransportError(err error, codes ...TransportErrorCode) bool {
	if err == nil {
		return false
	}
	var t *TransportError
	if !errors.As(err, &t) {
		return false
	}
	if len(codes) == 0 {
		return true
	}
	for _, code := range codes {
		if t.Reason == code {
			return true
		}
	}
	return false
}

func FromGRPCError(err error, opts ...teOpt) error {
	if err == nil {
		return nil
	}
	var t *TransportError
	if errors.As(err, &t) {
		return err
	}

	if s, ok := grpcStatus.FromError(err); ok {
		te := &TransportError{
			Reason:  FromGRPCCode(s.Code()),
			message: s.Message(),
			err:     s.Err(),
			details: s.Details(),
		}
		for _, o := range opts {
			o(te)
		}
		return te
	}
	return err
}

func MustPessimizeEndpoint(err error, codes ...TransportErrorCode) bool {
	switch {
	case err == nil:
		return false

	// all transport errors except selected codes
	case IsTransportError(err) && !IsTransportError(
		err,
		append(
			codes,
			TransportErrorResourceExhausted,
			TransportErrorOutOfRange,
			TransportErrorCanceled,
			TransportErrorDeadlineExceeded,
			TransportErrorInvalidArgument,
			TransportErrorNotFound,
			TransportErrorAlreadyExists,
			TransportErrorFailedPrecondition,
			TransportErrorUnimplemented,
			TransportErrorPermissionDenied,
		)...,
	):
		return true

	default:
		return false
	}
}
