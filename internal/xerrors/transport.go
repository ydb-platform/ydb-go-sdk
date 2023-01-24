package xerrors

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type transportError struct {
	status  *grpcStatus.Status
	err     error
	address string
}

func (e *transportError) GRPCStatus() *grpcStatus.Status {
	return e.status
}

func (e *transportError) isYdbError() {}

func (e *transportError) Code() int32 {
	return int32(e.status.Code())
}

func (e *transportError) Name() string {
	return "transport/" + e.status.Code().String()
}

type teOpt func(te *transportError)

func WithAddress(address string) teOpt {
	return func(te *transportError) {
		te.address = address
	}
}

func (e *transportError) Error() string {
	var b bytes.Buffer
	b.WriteString(e.Name())
	b.WriteString(fmt.Sprintf(" (%q", e.err.Error()))
	if len(e.address) > 0 {
		b.WriteString(fmt.Sprintf(", address: %q", e.address))
	}
	b.WriteString(")")
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

func (e *transportError) Type() Type {
	switch e.status.Code() {
	case
		grpcCodes.Aborted,
		grpcCodes.ResourceExhausted:
		return TypeRetryable
	case
		grpcCodes.Internal,
		grpcCodes.Canceled,
		grpcCodes.Unavailable:
		return TypeConditionallyRetryable
	default:
		return TypeUndefined
	}
}

func (e *transportError) BackoffType() backoff.Type {
	switch e.status.Code() {
	case
		grpcCodes.Internal,
		grpcCodes.Canceled,
		grpcCodes.Unavailable:
		return backoff.TypeFast
	case grpcCodes.ResourceExhausted:
		return backoff.TypeSlow
	default:
		return backoff.TypeNoBackoff
	}
}

func (e *transportError) MustDeleteSession() bool {
	switch e.status.Code() {
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
	if t := (*transportError)(nil); errors.As(err, &t) {
		if len(codes) == 0 {
			return true
		}
		for _, code := range codes {
			if t.status.Code() == code {
				return true
			}
		}
		return false
	}
	_, has := grpcStatus.FromError(err)
	return has
}

// Transport returns a new transport error with given options
func Transport(err error, opts ...teOpt) error {
	if err == nil {
		return nil
	}
	var t *transportError
	if errors.As(err, &t) {
		return err
	}
	if s, ok := grpcStatus.FromError(err); ok {
		te := &transportError{
			status: s,
			err:    err,
		}
		for _, o := range opts {
			if o != nil {
				o(te)
			}
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
