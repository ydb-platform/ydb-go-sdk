package ydb

import (
	"testing"

	"github.com/yandex-cloud/ydb-go-sdk/internal/tracetest"
)

func TestDriverTraceCompose(t *testing.T) {
	tracetest.TestCompose(t, composeDriverTrace, DriverTrace{})
}
