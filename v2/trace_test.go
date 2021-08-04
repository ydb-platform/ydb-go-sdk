package ydb

import (
	"testing"

	"github.com/yandex-cloud/ydb-go-sdk/v2/internal/tracetest"
)

func TestDriverTraceCompose(t *testing.T) {
	tracetest.TestCompose(t, composeDriverTrace, DriverTrace{})
}
