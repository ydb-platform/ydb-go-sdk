package traceutil

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracetest"
)

func TestDriverTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, trace.DriverTrace{}, "DriverTrace")
}
