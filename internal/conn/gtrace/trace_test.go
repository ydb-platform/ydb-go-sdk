package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithDriverPanicCallback)
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Driver](t,
		DriverOnInit,
		DriverOnWith,
		DriverOnClose,
		DriverOnPoolNew,
		DriverOnPoolRelease,
		DriverOnResolve,
		DriverOnConnStateChange,
		DriverOnConnInvoke,
		DriverOnConnNewStream,
		DriverOnConnStreamRecvMsg,
		DriverOnConnStreamSendMsg,
		DriverOnConnStreamCloseSend,
		DriverOnConnStreamFinish,
		DriverOnConnDial,
		DriverOnConnBan,
		DriverOnConnAllow,
		DriverOnConnPark,
		DriverOnConnClose,
		DriverOnRepeaterWakeUp,
		DriverOnBalancerInit,
		DriverOnBalancerClose,
		DriverOnBalancerChooseEndpoint,
		DriverOnBalancerClusterDiscoveryAttempt,
		DriverOnBalancerUpdate,
		DriverOnGetCredentials,
	)
}
