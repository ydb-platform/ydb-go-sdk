package table

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracetest"
)

func TestClientTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, Trace{}, "Trace")
}

func TestRetryTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, RetryTrace{}, "RetryTrace")
}

func TestSessionPoolTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, SessionPoolTrace{}, "SessionPoolTrace")
}
