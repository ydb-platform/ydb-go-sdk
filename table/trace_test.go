package table

import (
	"testing"

	"github.com/YandexDatabase/ydb-go-sdk/v2/internal/tracetest"
)

func TestClientTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, ClientTrace{}, "ClientTrace")
}

func TestRetryTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, RetryTrace{}, "RetryTrace")
}

func TestSessionPoolTrace(t *testing.T) {
	tracetest.TestSingleTrace(t, SessionPoolTrace{}, "SessionPoolTrace")
}
