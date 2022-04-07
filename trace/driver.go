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
		OnClose func(DriverCloseStartInfo) func(DriverCloseDoneInfo)

		// Network events
		OnNetRead  func(DriverNetReadStartInfo) func(DriverNetReadDoneInfo)
		OnNetWrite func(DriverNetWriteStartInfo) func(DriverNetWriteDoneInfo)
		OnNetDial  func(DriverNetDialStartInfo) func(DriverNetDialDoneInfo)
		OnNetClose func(DriverNetCloseStartInfo) func(DriverNetCloseDoneInfo)

		// Resolver events
		OnResolve func(DriverResolveStartInfo) func(DriverResolveDoneInfo)

		// Conn events
		OnConnStateChange func(DriverConnStateChangeStartInfo) func(DriverConnStateChangeDoneInfo)
		OnConnInvoke      func(DriverConnInvokeStartInfo) func(DriverConnInvokeDoneInfo)
		OnConnNewStream   func(
			DriverConnNewStreamStartInfo,
		) func(
			DriverConnNewStreamRecvInfo,
		) func(
			DriverConnNewStreamDoneInfo,
		)
		OnConnTake               func(DriverConnTakeStartInfo) func(DriverConnTakeDoneInfo)
		OnConnUsagesChange       func(DriverConnUsagesChangeInfo)
		OnConnStreamUsagesChange func(DriverConnStreamUsagesChangeInfo)
		OnConnPark               func(DriverConnParkStartInfo) func(DriverConnParkDoneInfo)
		OnConnClose              func(DriverConnCloseStartInfo) func(DriverConnCloseDoneInfo)
		OnConnRelease            func(DriverConnReleaseStartInfo) func(DriverConnReleaseDoneInfo)

		// Cluster events
		OnClusterInit   func(DriverClusterInitStartInfo) func(DriverClusterInitDoneInfo)
		OnClusterClose  func(DriverClusterCloseStartInfo) func(DriverClusterCloseDoneInfo)
		OnClusterGet    func(DriverClusterGetStartInfo) func(DriverClusterGetDoneInfo)
		OnClusterInsert func(DriverClusterInsertStartInfo) func(DriverClusterInsertDoneInfo)
		OnClusterRemove func(DriverClusterRemoveStartInfo) func(DriverClusterRemoveDoneInfo)
		OnPessimizeNode func(DriverPessimizeNodeStartInfo) func(DriverPessimizeNodeDoneInfo)

		// Repeater events
		OnRepeaterWakeUp func(DriverRepeaterTickStartInfo) func(DriverRepeaterTickDoneInfo)

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
	DriverClusterInsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	DriverClusterInsertDoneInfo struct {
		Inserted bool
		State    ConnState
	}
	DriverClusterRemoveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	DriverClusterRemoveDoneInfo struct {
		Removed bool
		State   ConnState
	}
	DriverConnStateChangeStartInfo struct {
		Endpoint EndpointInfo
		State    ConnState
	}
	DriverConnStateChangeDoneInfo struct {
		State ConnState
	}
	DriverResolveStartInfo struct {
		Target   string
		Resolved []string
	}
	DriverResolveDoneInfo struct {
		Error error
	}
	DriverNetReadStartInfo struct {
		Address string
		Buffer  int
	}
	DriverNetReadDoneInfo struct {
		Received int
		Error    error
	}
	DriverNetWriteStartInfo struct {
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
		Address string
	}
	DriverNetDialDoneInfo struct {
		Error error
	}
	DriverNetCloseStartInfo struct {
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
		Endpoint EndpointInfo
	}
	DriverConnTakeDoneInfo struct {
		Error error
	}
	DriverConnParkStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
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
		Endpoint EndpointInfo
	}
	DriverConnCloseDoneInfo struct {
		Error error
	}
	DriverConnReleaseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	DriverConnReleaseDoneInfo struct {
		Error error
	}
	DriverConnUsagesChangeInfo struct {
		Endpoint EndpointInfo
		Usages   int
	}
	DriverConnStreamUsagesChangeInfo struct {
		Endpoint EndpointInfo
		Usages   int
	}
	DriverConnInvokeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		Method   Method
	}
	DriverConnInvokeDoneInfo struct {
		Error  error
		Issues []Issue
		OpID   string
		State  ConnState
	}
	DriverConnNewStreamStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		Method   Method
	}
	DriverConnNewStreamRecvInfo struct {
		Error error
	}
	DriverConnNewStreamDoneInfo struct {
		State ConnState
		Error error
	}
	DriverClusterInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DriverClusterInitDoneInfo struct {
		Error error
	}
	DriverClusterCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DriverClusterCloseDoneInfo struct {
		Error error
	}
	DriverClusterGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DriverClusterGetDoneInfo struct {
		Endpoint EndpointInfo
		Error    error
	}
	DriverPessimizeNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		State    ConnState
		Cause    error
	}
	DriverPessimizeNodeDoneInfo struct {
		State ConnState
	}
	DriverRepeaterTickStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Name    string
		Event   string
	}
	DriverRepeaterTickDoneInfo struct {
		Error error
	}
	DriverGetCredentialsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
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
		Endpoint string
		Database string
		Secure   bool
	}
	DriverInitDoneInfo struct {
		Error error
	}
	DriverCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	DriverCloseDoneInfo struct {
		Error error
	}
)
