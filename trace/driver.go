package trace

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	// Driver specified trace of common driver activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	Driver struct {
		// Driver runtime events
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnInit func(DriverInitStartInfo) func(DriverInitDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnWith func(DriverWithStartInfo) func(DriverWithDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnClose func(DriverCloseStartInfo) func(DriverCloseDoneInfo)

		// Pool of connections
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnPoolNew func(DriverConnPoolNewStartInfo) func(DriverConnPoolNewDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnPoolRelease func(DriverConnPoolReleaseStartInfo) func(DriverConnPoolReleaseDoneInfo)

		// Resolver events
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnResolve func(DriverResolveStartInfo) func(DriverResolveDoneInfo)

		// Conn events
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnConnStateChange func(DriverConnStateChangeStartInfo) func(DriverConnStateChangeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnConnInvoke func(DriverConnInvokeStartInfo) func(DriverConnInvokeDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnConnNewStream func(DriverConnNewStreamStartInfo) func(DriverConnNewStreamDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnConnStreamRecvMsg func(DriverConnStreamRecvMsgStartInfo) func(DriverConnStreamRecvMsgDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnConnStreamSendMsg   func(DriverConnStreamSendMsgStartInfo) func(DriverConnStreamSendMsgDoneInfo)
		OnConnStreamCloseSend func(DriverConnStreamCloseSendStartInfo) func(DriverConnStreamCloseSendDoneInfo)
		OnConnDial            func(DriverConnDialStartInfo) func(DriverConnDialDoneInfo)
		OnConnBan             func(DriverConnBanStartInfo) func(DriverConnBanDoneInfo)
		OnConnAllow           func(DriverConnAllowStartInfo) func(DriverConnAllowDoneInfo)
		OnConnPark            func(DriverConnParkStartInfo) func(DriverConnParkDoneInfo)
		OnConnClose           func(DriverConnCloseStartInfo) func(DriverConnCloseDoneInfo)

		// Repeater events
		OnRepeaterWakeUp func(DriverRepeaterWakeUpStartInfo) func(DriverRepeaterWakeUpDoneInfo)

		// Balancer events
		OnBalancerInit func(DriverBalancerInitStartInfo) func(DriverBalancerInitDoneInfo)

		OnBalancerClose          func(DriverBalancerCloseStartInfo) func(DriverBalancerCloseDoneInfo)
		OnBalancerChooseEndpoint func(
			DriverBalancerChooseEndpointStartInfo,
		) func(
			DriverBalancerChooseEndpointDoneInfo,
		)
		OnBalancerClusterDiscoveryAttempt func(
			DriverBalancerClusterDiscoveryAttemptStartInfo,
		) func(
			DriverBalancerClusterDiscoveryAttemptDoneInfo,
		)
		OnBalancerUpdate func(DriverBalancerUpdateStartInfo) func(DriverBalancerUpdateDoneInfo)

		// Credentials events
		OnGetCredentials func(DriverGetCredentialsStartInfo) func(DriverGetCredentialsDoneInfo)
	}
)

// Method represents rpc method.
type Method string

// Name returns the rpc method name.
func (m Method) Name() (s string) {
	_, s = m.Split()

	return
}

// Service returns the rpc service name.
func (m Method) Service() (s string) {
	s, _ = m.Split()

	return
}

// Issue declare interface of operation error issues
type Issue interface {
	GetMessage() string
	GetIssueCode() uint32
	GetSeverity() uint32
}

// Split returns service name and method.
func (m Method) Split() (service, method string) {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m), string(m)
	}

	return strings.TrimPrefix(string(m[:i]), "/"), string(m[i+1:])
}

type ConnState interface {
	fmt.Stringer

	IsValid() bool
	Code() int
}

type EndpointInfo interface {
	fmt.Stringer

	NodeID() uint32
	Address() string
	LocalDC() bool
	Location() string
	LoadFactor() float32
	LastUpdated() time.Time
}

type (
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnStateChangeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		State    ConnState
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnStateChangeDoneInfo struct {
		State ConnState
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverResolveStartInfo struct {
		Call     call
		Target   string
		Resolved []string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverResolveDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverBalancerUpdateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		NeedLocalDC bool
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverBalancerUpdateDoneInfo struct {
		Endpoints []EndpointInfo
		Added     []EndpointInfo
		Dropped   []EndpointInfo
		LocalDC   string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverBalancerClusterDiscoveryAttemptStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Address string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverBalancerClusterDiscoveryAttemptDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetReadStartInfo struct {
		Call    call
		Address string
		Buffer  int
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetReadDoneInfo struct {
		Received int
		Error    error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetWriteStartInfo struct {
		Call    call
		Address string
		Bytes   int
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetWriteDoneInfo struct {
		Sent  int
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetDialStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Address string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetDialDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetCloseStartInfo struct {
		Call    call
		Address string
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverNetCloseDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnTakeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnTakeDoneInfo struct {
		Error error
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnDialStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	DriverConnDialDoneInfo struct {
		Error error
	}
	DriverConnParkStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
	DriverConnParkDoneInfo struct {
		Error error
	}
	DriverConnCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
	DriverConnCloseDoneInfo struct {
		Error error
	}
	DriverConnBanStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		State    ConnState
		Cause    error
	}
	DriverConnBanDoneInfo struct {
		State ConnState
	}
	DriverConnAllowStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		State    ConnState
	}
	DriverConnAllowDoneInfo struct {
		State ConnState
	}
	DriverConnInvokeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		Method   Method
	}
	DriverConnInvokeDoneInfo struct {
		Error    error
		Issues   []Issue
		OpID     string
		State    ConnState
		Metadata map[string][]string
	}
	DriverConnNewStreamStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		Method   Method
	}
	DriverConnNewStreamDoneInfo struct {
		Error error
		State ConnState
	}
	DriverConnStreamRecvMsgStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnStreamRecvMsgDoneInfo struct {
		Error error
	}
	DriverConnStreamSendMsgStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnStreamSendMsgDoneInfo struct {
		Error error
	}
	DriverConnStreamCloseSendStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnStreamCloseSendDoneInfo struct {
		Error error
	}
	DriverBalancerInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Name    string
	}
	DriverBalancerInitDoneInfo struct {
		Error error
	}
	DriverBalancerDialEntrypointStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Address string
	}
	DriverBalancerDialEntrypointDoneInfo struct {
		Error error
	}
	DriverBalancerCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverBalancerCloseDoneInfo struct {
		Error error
	}
	DriverBalancerChooseEndpointStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverBalancerChooseEndpointDoneInfo struct {
		Endpoint EndpointInfo
		Error    error
	}
	DriverRepeaterWakeUpStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Name    string
		Event   string
	}
	DriverRepeaterWakeUpDoneInfo struct {
		Error error
	}
	DriverGetCredentialsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverGetCredentialsDoneInfo struct {
		Token string
		Error error
	}
	DriverInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint string
		Database string
		Secure   bool
	}
	DriverInitDoneInfo struct {
		Error error
	}
	DriverWithStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint string
		Database string
		Secure   bool
	}
	DriverWithDoneInfo struct {
		Error error
	}
	DriverConnPoolNewStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnPoolNewDoneInfo      struct{}
	DriverConnPoolReleaseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnPoolReleaseDoneInfo struct {
		Error error
	}
	DriverCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverCloseDoneInfo struct {
		Error error
	}
)
