package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithDiscoveryPanicCallback)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Discovery](t,
		DiscoveryOnDiscover,
		DiscoveryOnWhoAmI,
	)
}
