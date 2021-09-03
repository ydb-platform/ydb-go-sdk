package ydb

import (
	"testing"

	"github.com/YandexDatabase/ydb-go-sdk/v3/internal/tracetest"
)

func TestDriverTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, DriverTrace{}, "DriverTrace")
}
