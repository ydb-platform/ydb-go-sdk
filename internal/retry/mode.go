package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

type Mode interface {
	MustRetry(isOperationIdempotent bool) bool
	StatusCode() int64
	MustBackoff() bool
	BackoffType() backoff.Type
	MustDeleteSession() bool
}

// Mode reports whether operation is able retried and with which properties.
type mode struct {
	statusCode      int64
	operationStatus operation.Status
	backoff         backoff.Type
	deleteSession   bool
}

func NewMode(statusCode int64, operationStatus operation.Status, backoff backoff.Type, deleteSession bool) Mode {
	return mode{
		statusCode:      statusCode,
		operationStatus: operationStatus,
		backoff:         backoff,
		deleteSession:   deleteSession,
	}
}

func (m mode) MustRetry(isOperationIdempotent bool) bool {
	switch m.operationStatus {
	case operation.Finished:
		return false
	case operation.Undefined:
		return isOperationIdempotent
	default:
		return true
	}
}

func (m mode) StatusCode() int64 { return m.statusCode }

func (m mode) MustBackoff() bool { return m.backoff&backoff.TypeAny != 0 }

func (m mode) BackoffType() backoff.Type { return m.backoff }

func (m mode) MustDeleteSession() bool { return m.deleteSession }
