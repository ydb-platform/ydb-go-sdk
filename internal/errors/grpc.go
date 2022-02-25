package errors

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcError looks like origin grpc error
type grpcError struct {
	status *status.Status
	err    error
}

func (e *grpcError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.status.String(), e.err)
	}
	return e.status.String()
}

func (e *grpcError) Unwrap() error {
	return e.err
}

func (e *grpcError) GRPCStatus() *status.Status {
	return e.status
}

type grpcErrorOptionsHolder struct {
	msg string
	err error
}

type grpcErrorOption func(h *grpcErrorOptionsHolder)

func WithMsg(msg string) grpcErrorOption {
	return func(h *grpcErrorOptionsHolder) {
		h.msg = msg
	}
}

func WithErr(err error) grpcErrorOption {
	return func(h *grpcErrorOptionsHolder) {
		h.err = err
	}
}

func NewGrpcError(code codes.Code, opts ...grpcErrorOption) error {
	h := &grpcErrorOptionsHolder{}
	for _, o := range opts {
		o(h)
	}
	return &grpcError{
		status: status.New(code, h.msg),
		err:    h.err,
	}
}
