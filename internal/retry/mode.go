package retry

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

// Mode reports whether operation is able retried and with which properties.
type Mode struct {
	statusCode      int64
	operationStatus operation.Status
	backoff         backoff.Type
	deleteSession   bool
}

func NewMode(statusCode int64, operationStatus operation.Status, backoff backoff.Type, deleteSession bool) Mode {
	return Mode{
		statusCode:      statusCode,
		operationStatus: operationStatus,
		backoff:         backoff,
		deleteSession:   deleteSession,
	}
}

func (m Mode) MustRetry(isOperationIdempotent bool) bool {
	switch m.operationStatus {
	case operation.Finished:
		return false
	case operation.Undefined:
		return isOperationIdempotent
	default:
		return true
	}
}

func (m Mode) StatusCode() int64 { return m.statusCode }

func (m Mode) MustBackoff() bool { return m.backoff&backoff.TypeAny != 0 }

func (m Mode) BackoffType() backoff.Type { return m.backoff }

func (m Mode) MustDeleteSession() bool { return m.deleteSession }
