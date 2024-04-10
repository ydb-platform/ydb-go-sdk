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
	Driver struct {
		// Driver runtime events
		OnInit  func(DriverInitStartInfo) func(DriverInitDoneInfo)
		OnWith  func(DriverWithStartInfo) func(DriverWithDoneInfo)
		OnClose func(DriverCloseStartInfo) func(DriverCloseDoneInfo)

		// Pool of connections
		OnPoolNew    func(DriverConnPoolNewStartInfo) func(DriverConnPoolNewDoneInfo)
		OnPoolAttach func(DriverConnPoolAttachStartInfo) func(DriverConnPoolAttachDoneInfo)
		OnPoolDetach func(DriverConnPoolDetachStartInfo) func(DriverConnPoolDetachDoneInfo)

		// Deprecated
		OnPoolRelease func(DriverConnPoolReleaseStartInfo) func(DriverConnPoolReleaseDoneInfo)

		// Resolver events
		OnResolve func(DriverResolveStartInfo) func(DriverResolveDoneInfo)

		// Conn events
		OnConnStateChange     func(DriverConnStateChangeStartInfo) func(DriverConnStateChangeDoneInfo)
		OnConnInvoke          func(DriverConnInvokeStartInfo) func(DriverConnInvokeDoneInfo)
		OnConnNewStream       func(DriverConnNewStreamStartInfo) func(DriverConnNewStreamDoneInfo)
		OnConnStreamRecvMsg   func(DriverConnStreamRecvMsgStartInfo) func(DriverConnStreamRecvMsgDoneInfo)
		OnConnStreamSendMsg   func(DriverConnStreamSendMsgStartInfo) func(DriverConnStreamSendMsgDoneInfo)
		OnConnStreamCloseSend func(DriverConnStreamCloseSendStartInfo) func(DriverConnStreamCloseSendDoneInfo)
		OnConnDial            func(DriverConnDialStartInfo) func(DriverConnDialDoneInfo)
		OnConnBan             func(DriverConnBanStartInfo) func(DriverConnBanDoneInfo)
		OnConnUnban           func(DriverConnUnbanStartInfo) func(DriverConnUnbanDoneInfo)
		OnConnPark            func(DriverConnParkStartInfo) func(DriverConnParkDoneInfo)
		OnConnClose           func(DriverConnCloseStartInfo) func(DriverConnCloseDoneInfo)

		// Repeater events
		OnRepeaterWakeUp func(DriverRepeaterWakeUpStartInfo) func(DriverRepeaterWakeUpDoneInfo)

		// Balancer events
		OnBalancerInit    func(DriverBalancerInitStartInfo) func(DriverBalancerInitDoneInfo)
		OnBalancerClose   func(DriverBalancerCloseStartInfo) func(DriverBalancerCloseDoneInfo)
		OnBalancerGetConn func(
			DriverBalancerGetConnStartInfo,
		) func(
			DriverBalancerGetConnDoneInfo,
		)
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
		OnBalancerUpdate        func(DriverBalancerUpdateStartInfo) func(DriverBalancerUpdateDoneInfo)
		OnBalancerMarkConnAsBad func(DriverBalancerMarkConnAsBadStartInfo) func(DriverBalancerMarkConnAsBadDoneInfo)

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
}

type EndpointInfo interface {
	fmt.Stringer

	NodeID() uint32
	Address() string
	Location() string
	LoadFactor() float32
	LastUpdated() time.Time
}

type (
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
	DriverConnStateChangeDoneInfo struct {
		State ConnState
	}
	DriverResolveStartInfo struct {
		Call     call
		Target   string
		Resolved []string
	}
	DriverResolveDoneInfo struct {
		Error error
	}
	DriverBalancerUpdateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context     *context.Context
		Call        call
		NeedLocalDC bool
	}
	DriverBalancerUpdateDoneInfo struct {
		Endpoints []EndpointInfo
		Added     []EndpointInfo
		Dropped   []EndpointInfo
		LocalDC   string
	}
	DriverBalancerMarkConnAsBadStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call

		Endpoint EndpointInfo
		Cause    error
	}
	DriverBalancerMarkConnAsBadDoneInfo struct {
		Prefer   []EndpointInfo
		Fallback []EndpointInfo
	}
	DriverBalancerClusterDiscoveryAttemptStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Address string
	}
	DriverBalancerClusterDiscoveryAttemptDoneInfo struct {
		Error error
	}
	DriverNetReadStartInfo struct {
		Call    call
		Address string
		Buffer  int
	}
	DriverNetReadDoneInfo struct {
		Received int
		Error    error
	}
	DriverNetWriteStartInfo struct {
		Call    call
		Address string
		Bytes   int
	}
	DriverNetWriteDoneInfo struct {
		Sent  int
		Error error
	}
	DriverNetDialStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
		Address string
	}
	DriverNetDialDoneInfo struct {
		Error error
	}
	DriverNetCloseStartInfo struct {
		Call    call
		Address string
	}
	DriverNetCloseDoneInfo struct {
		Error error
	}
	DriverConnTakeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
	DriverConnTakeDoneInfo struct {
		Error error
	}
	DriverConnDialStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
	}
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
	DriverConnUnbanStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Call     call
		Endpoint EndpointInfo
		State    ConnState
	}
	DriverConnUnbanDoneInfo struct {
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
	DriverBalancerGetConnStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverBalancerGetConnDoneInfo struct {
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
	DriverConnPoolAttachStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnPoolAttachDoneInfo struct {
		Error error
	}
	DriverConnPoolDetachStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Call    call
	}
	DriverConnPoolDetachDoneInfo struct {
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
