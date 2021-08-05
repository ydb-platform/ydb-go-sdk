package ydb

import (
	"testing"

	"github.com/yandex-cloud/ydb-go-sdk/internal/tracetest"
)

func TestDriverTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, DriverTrace{}, "DriverTrace")
}
