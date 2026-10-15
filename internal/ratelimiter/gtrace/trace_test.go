package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithRatelimiterPanicCallback)
}
