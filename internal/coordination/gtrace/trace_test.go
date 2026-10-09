package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithCoordinationPanicCallback)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Coordination](t,
		CoordinationOnNew,
		CoordinationOnCreateNode,
		CoordinationOnAlterNode,
		CoordinationOnDropNode,
		CoordinationOnDescribeNode,
		CoordinationOnSession,
		CoordinationOnClose,
		CoordinationOnSessionNewStream,
		CoordinationOnSessionStarted,
		CoordinationOnSessionStartTimeout,
		CoordinationOnSessionKeepAliveTimeout,
		CoordinationOnSessionStopped,
		CoordinationOnSessionStopTimeout,
		CoordinationOnSessionClientTimeout,
		CoordinationOnSessionServerExpire,
		CoordinationOnSessionServerError,
		CoordinationOnSessionReceive,
		CoordinationOnSessionReceiveUnexpected,
		CoordinationOnSessionStop,
		CoordinationOnSessionStart,
		CoordinationOnSessionSend,
	)
}
