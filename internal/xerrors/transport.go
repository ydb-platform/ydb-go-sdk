package xerrors

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

type transportError struct {
	code    grpcCodes.Code
	message string
	err     error
	details []interface{}
	address string
}

func (e *transportError) isYdbError() {}

func (e *transportError) Code() int32 {
	return int32(e.code)
}

func (e *transportError) Name() string {
	return e.code.String()
}

type teOpt func(te *transportError)

func WithCode(code grpcCodes.Code) teOpt {
	return func(te *transportError) {
		te.code = code
	}
}

func WithAddress(address string) teOpt {
	return func(te *transportError) {
		te.address = address
	}
}

// Transport returns a new transport error with given options
func Transport(opts ...teOpt) error {
	te := &transportError{}
	for _, f := range opts {
		f(te)
	}
	return WithStackTrace(fmt.Errorf("%w", te), WithSkipDepth(1))
}

func (e *transportError) Error() string {
	var b bytes.Buffer
	b.WriteString("transport error: ")
	b.WriteString(e.code.String())
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

func (e *transportError) Unwrap() error {
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

func (e *transportError) OperationStatus() OperationStatus {
	switch e.code {
	case
		grpcCodes.Aborted,
		grpcCodes.ResourceExhausted:
		return OperationNotFinished
	case
		grpcCodes.Internal,
		grpcCodes.Canceled,
		grpcCodes.Unavailable:
		return OperationStatusUndefined
	default:
		return OperationFinished
	}
}

func (e *transportError) BackoffType() BackoffType {
	switch e.code {
	case
		grpcCodes.Internal,
		grpcCodes.Canceled,
		grpcCodes.Unavailable:
		return BackoffTypeFastBackoff
	case
		grpcCodes.ResourceExhausted:
		return BackoffTypeSlowBackoff
	default:
		return BackoffTypeNoBackoff
	}
}

func (e *transportError) MustDeleteSession() bool {
	switch e.code {
	case
		grpcCodes.ResourceExhausted,
		grpcCodes.OutOfRange:
		return false
	default:
		return true
	}
}

// IsTransportError reports whether err is transportError with given grpc codes
func IsTransportError(err error, codes ...grpcCodes.Code) bool {
	if err == nil {
		return false
	}
	var t *transportError
	if !errors.As(err, &t) {
		return false
	}
	if len(codes) == 0 {
		return true
	}
	for _, code := range codes {
		if t.code == code {
			return true
		}
	}
	return false
}

func FromGRPCError(err error, opts ...teOpt) error {
	if err == nil {
		return nil
	}
	var t *transportError
	if errors.As(err, &t) {
		return err
	}

	if s, ok := grpcStatus.FromError(err); ok {
		te := &transportError{
			code:    s.Code(),
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

func MustPessimizeEndpoint(err error, codes ...grpcCodes.Code) bool {
	switch {
	case err == nil:
		return false

	// all transport errors except selected codes
	case IsTransportError(err) && !IsTransportError(
		err,
		append(
			codes,
			grpcCodes.ResourceExhausted,
			grpcCodes.OutOfRange,
		)...,
	):
		return true

	default:
		return false
	}
}

func TransportError(err error) Error {
	var t *transportError
	if errors.As(err, &t) {
		return t
	}
	return nil
}
