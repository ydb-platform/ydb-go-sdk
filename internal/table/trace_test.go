package table

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracetest"
)

func TestClientTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, Trace{}, "Trace")
}

func TestSessionPoolTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, SessionPoolTrace{}, "SessionPoolTrace")
}
