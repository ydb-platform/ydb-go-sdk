package trace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracetest"
)

func TestClientTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, Table{}, "Table")
}

func TestSessionPoolTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, SessionPoolTrace{}, "SessionPoolTrace")
}
