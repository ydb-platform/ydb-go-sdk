package errors

import (
	"google.golang.org/grpc/status"
)

// grpcError looks like origin grpc error
type grpcError struct {
	status *status.Status
	err    error
}

func (e *grpcError) isYdbError() {}

func (e *grpcError) Error() (s string) {
	if e.err != nil {
		s += e.err.Error() + ": "
	}
	return s + e.status.String()
}

func (e *grpcError) Unwrap() error {
	return e.err
}

func (e *grpcError) GRPCStatus() *status.Status {
	return e.status
}

type grpcErrorOption func(e *grpcError)

func WithStatus(s *status.Status) grpcErrorOption {
	return func(e *grpcError) {
		e.status = s
	}
}

func WithErr(err error) grpcErrorOption {
	return func(e *grpcError) {
		e.err = err
	}
}

func NewGrpcError(opts ...grpcErrorOption) error {
	e := &grpcError{}
	for _, o := range opts {
		o(e)
	}
	return e
}
