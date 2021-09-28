package trace

//go:generate gtrace

import (
	"context"
	"strings"
	"time"
)

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Driver struct {
		// Conn events
		OnConnNew         func(Endpoint Endpoint)
		OnConnDial        func(ConnDialStartInfo) func(ConnDialDoneInfo)
		OnConnDisconnect  func(ConnDisconnectStartInfo) func(ConnDisconnectDoneInfo)
		OnConnStateChenge func(ConnStateChangeStartInfo) func(ConnStateChangeDoneInfo)

		// Cluster events
		OnClusterGet    func(ClusterGetStartInfo) func(ClusterGetDoneInfo)
		OnClusterInsert func(ClusterInsertStartInfo) func(ClusterInsertDoneInfo)
		OnClusterUpdate func(ClusterUpdateStartInfo) func(ClusterUpdateDoneInfo)
		OnClusterRemove func(ClusterRemoveStartInfo) func(ClusterRemoveDoneInfo)
		OnPessimizeNode func(PessimizeNodeStartInfo) func(PessimizeNodeDoneInfo)

		// Credentials events
		OnGetCredentials func(GetCredentialsStartInfo) func(GetCredentialsDoneInfo)

		// Discovery events
		OnDiscovery func(DiscoveryStartInfo) func(DiscoveryDoneInfo)

		// RPC events
		OnOperation func(OperationStartInfo) func(OperationDoneInfo)
		OnStream    func(StreamStartInfo) func(StreamRecvDoneInfo) func(StreamDoneInfo)
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

type Issue interface {
	GetMessage() string
	GetIssueCode() uint32
	GetSeverity() uint32
}

type OperationParams interface {
	GetTimeout() time.Duration
	GetCancelAfter() time.Duration
	GetMode() string
}

// Split returns service name and method.
func (m Method) Split() (service, method string) {
	i := strings.LastIndex(string(m), "/")
	if i == -1 {
		return string(m), string(m)
	}
	return strings.TrimPrefix(string(m[:i]), "/"), string(m[i+1:])
}

type Endpoint interface {
	Address() string
}

type ConnState interface {
	String() string
	Code() int
}

type (
	ClusterInsertStartInfo struct {
		Endpoint Endpoint
	}
	ClusterInsertDoneInfo struct {
		ClusterSize int
		State       ConnState
	}
	ClusterUpdateStartInfo struct {
		Endpoint Endpoint
	}
	ClusterUpdateDoneInfo struct {
		State ConnState
	}
	ClusterRemoveStartInfo struct {
		Endpoint Endpoint
	}
	ClusterRemoveDoneInfo struct {
		ClusterSize int
		State       ConnState
	}
	ConnDisconnectStartInfo struct {
		Endpoint Endpoint
		State    ConnState
	}
	ConnDisconnectDoneInfo struct {
		Error error
		State ConnState
	}
	ConnStateChangeStartInfo struct {
		Endpoint Endpoint
		State    ConnState
	}
	ConnStateChangeDoneInfo struct {
		State ConnState
	}
	ConnDialStartInfo struct {
		Context  context.Context
		Endpoint Endpoint
		State    ConnState
	}
	ConnDialDoneInfo struct {
		Error error
		State ConnState
	}
	ClusterGetStartInfo struct {
		Context context.Context
	}
	ClusterGetDoneInfo struct {
		Endpoint Endpoint
		Error    error
	}
	PessimizeNodeStartInfo struct {
		Context  context.Context
		Endpoint Endpoint
		State    ConnState
		Cause    error
	}
	PessimizeNodeDoneInfo struct {
		State ConnState
		Error error
	}
	GetCredentialsStartInfo struct {
		Context context.Context
	}
	GetCredentialsDoneInfo struct {
		TokenOk bool
		Error   error
	}
	DiscoveryStartInfo struct {
		Context context.Context
	}
	DiscoveryDoneInfo struct {
		Endpoints map[Endpoint]ConnState
		Error     error
	}
	OperationStartInfo struct {
		Context  context.Context
		Endpoint Endpoint
		Method   Method
		Params   OperationParams
	}
	OperationDoneInfo struct {
		OpID   string
		Issues []Issue
		Error  error
	}
	StreamStartInfo struct {
		Context  context.Context
		Endpoint Endpoint
		Method   Method
	}
	StreamRecvDoneInfo struct {
		Error error
	}
	StreamDoneInfo struct {
		Error error
	}
)
