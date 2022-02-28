package trace

// tool gtrace used from repository github.com/asmyasnikov/cmd/gtrace

//go:generate gtrace

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Driver struct {
		// Driver runtime events
		OnInit  func(InitStartInfo) func(InitDoneInfo)
		OnClose func(CloseStartInfo) func(CloseDoneInfo)

		// Network events
		OnNetRead  func(NetReadStartInfo) func(NetReadDoneInfo)
		OnNetWrite func(NetWriteStartInfo) func(NetWriteDoneInfo)
		OnNetDial  func(NetDialStartInfo) func(NetDialDoneInfo)
		OnNetClose func(NetCloseStartInfo) func(NetCloseDoneInfo)

		// Resolver events
		OnResolve func(ResolveStartInfo) func(ResolveDoneInfo)

		// Conn events
		OnConnStateChange  func(ConnStateChangeStartInfo) func(ConnStateChangeDoneInfo)
		OnConnInvoke       func(ConnInvokeStartInfo) func(ConnInvokeDoneInfo)
		OnConnNewStream    func(ConnNewStreamStartInfo) func(ConnNewStreamRecvInfo) func(ConnNewStreamDoneInfo)
		OnConnTake         func(ConnTakeStartInfo) func(ConnTakeDoneInfo)
		OnConnUsagesChange func(ConnUsagesChangeInfo)

		// Cluster events
		OnClusterInit   func(ClusterInitStartInfo) func(ClusterInitDoneInfo)
		OnClusterClose  func(ClusterCloseStartInfo) func(ClusterCloseDoneInfo)
		OnClusterGet    func(ClusterGetStartInfo) func(ClusterGetDoneInfo)
		OnClusterInsert func(ClusterInsertStartInfo) func(ClusterInsertDoneInfo)
		OnClusterUpdate func(ClusterUpdateStartInfo) func(ClusterUpdateDoneInfo)
		OnClusterRemove func(ClusterRemoveStartInfo) func(ClusterRemoveDoneInfo)
		OnPessimizeNode func(PessimizeNodeStartInfo) func(PessimizeNodeDoneInfo)

		// Credentials events
		OnGetCredentials func(GetCredentialsStartInfo) func(GetCredentialsDoneInfo)
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

// Issue interface
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
	LastUpdated() time.Time
}

type (
	ClusterInsertStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	ClusterInsertDoneInfo struct {
		State ConnState
	}
	ClusterUpdateStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	ClusterUpdateDoneInfo struct {
		State ConnState
	}
	ClusterRemoveStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	ClusterRemoveDoneInfo struct {
		State ConnState
	}
	ConnStateChangeStartInfo struct {
		Endpoint EndpointInfo
		State    ConnState
	}
	ConnStateChangeDoneInfo struct {
		State ConnState
	}
	ResolveStartInfo struct {
		Target   string
		Resolved []string
	}
	ResolveDoneInfo struct {
		Error error
	}
	NetReadStartInfo struct {
		Address string
		Buffer  int
	}
	NetReadDoneInfo struct {
		Received int
		Error    error
	}
	NetWriteStartInfo struct {
		Address string
		Bytes   int
	}
	NetWriteDoneInfo struct {
		Sent  int
		Error error
	}
	NetDialStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
		Address string
	}
	NetDialDoneInfo struct {
		Error error
	}
	NetCloseStartInfo struct {
		Address string
	}
	NetCloseDoneInfo struct {
		Error error
	}
	ConnTakeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
	}
	ConnTakeDoneInfo struct {
		Error error
	}
	ConnUsagesChangeInfo struct {
		Endpoint EndpointInfo
		Usages   int
	}
	ConnInvokeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		Method   Method
	}
	ConnInvokeDoneInfo struct {
		Error  error
		Issues []Issue
		OpID   string
		State  ConnState
	}
	ConnNewStreamStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		Method   Method
	}
	ConnNewStreamRecvInfo struct {
		Error error
	}
	ConnNewStreamDoneInfo struct {
		State ConnState
		Error error
	}
	ClusterInitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	ClusterInitDoneInfo   struct{}
	ClusterCloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	ClusterCloseDoneInfo struct {
		Error error
	}
	ClusterGetStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	ClusterGetDoneInfo struct {
		Endpoint EndpointInfo
		Error    error
	}
	PessimizeNodeStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint EndpointInfo
		State    ConnState
		Cause    error
	}
	PessimizeNodeDoneInfo struct {
		State ConnState
	}
	GetCredentialsStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	GetCredentialsDoneInfo struct {
		Token string
		Error error
	}
	InitStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context  *context.Context
		Endpoint string
		Database string
		Secure   bool
	}
	InitDoneInfo struct {
		Error error
	}
	CloseStartInfo struct {
		// Context make available context in trace callback function.
		// Pointer to context provide replacement of context in trace callback function.
		// Warning: concurrent access to pointer on client side must be excluded.
		// Safe replacement of context are provided only inside callback function
		Context *context.Context
	}
	CloseDoneInfo struct {
		Error error
	}
)
