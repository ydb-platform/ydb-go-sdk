package table

import (
	"testing"

	"github.com/yandex-cloud/ydb-go-sdk/v2/internal/tracetest"
)

func TestComposeClientTrace(t *testing.T) {
	tracetest.TestCompose(t, composeClientTrace, ClientTrace{})
}

func TestComposeSessionPoolTrace(t *testing.T) {
	tracetest.TestCompose(t, composeSessionPoolTrace, SessionPoolTrace{})
}
