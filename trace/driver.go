package trace

//go:generate gtrace

import (
	"context"
	"strings"
)

type (
	//gtrace:gen
	//gtrace:set Shortcut
	Driver struct {
		// Conn events
		OnConnNew         func(ConnNewStartInfo) func(ConnNewDoneInfo)
		OnConnClose       func(ConnCloseStartInfo) func(ConnCloseDoneInfo)
		OnConnDial        func(ConnDialStartInfo) func(ConnDialDoneInfo)
		OnConnDisconnect  func(ConnDisconnectStartInfo) func(ConnDisconnectDoneInfo)
		OnConnStateChange func(ConnStateChangeStartInfo) func(ConnStateChangeDoneInfo)
		OnConnInvoke      func(ConnInvokeStartInfo) func(ConnInvokeDoneInfo)
		OnConnNewStream   func(ConnNewStreamStartInfo) func(ConnNewStreamRecvInfo) func(ConnNewStreamDoneInfo)

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
	String() string
	Code() int
}

type Location int

const (
	LocationUnknown = Location(iota)
	LocationLocal
	LocationRemote
)

func (l Location) String() string {
	switch l {
	case LocationLocal:
		return "local"
	case LocationRemote:
		return "remote"
	default:
		return "unknown"
	}
}

type (
	ClusterInsertStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
	}
	ClusterInsertDoneInfo struct {
		State    ConnState
		Location Location
	}
	ClusterUpdateStartInfo struct {
		Context context.Context
		Address string
	}
	ClusterUpdateDoneInfo struct {
		State ConnState
	}
	ClusterRemoveStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
	}
	ClusterRemoveDoneInfo struct {
		State ConnState
	}
	ConnDisconnectStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		State    ConnState
	}
	ConnDisconnectDoneInfo struct {
		State ConnState
		Error error
	}
	ConnStateChangeStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		State    ConnState
	}
	ConnStateChangeDoneInfo struct {
		State ConnState
	}
	ConnNewStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
	}
	ConnNewDoneInfo struct {
		State ConnState
	}
	ConnCloseStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		State    ConnState
	}
	ConnCloseDoneInfo struct {
	}
	ConnDialStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		State    ConnState
	}
	ConnDialDoneInfo struct {
		Error error
		State ConnState
	}
	ConnInvokeStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		Method   Method
	}
	ConnInvokeDoneInfo struct {
		Error  error
		Issues []Issue
		OpID   string
		State  ConnState
	}
	ConnNewStreamStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
		Method   Method
	}
	ConnNewStreamRecvInfo struct {
		Error error
	}
	ConnNewStreamDoneInfo struct {
		State ConnState
		Error error
	}
	ClusterGetStartInfo struct {
		Context context.Context
	}
	ClusterGetDoneInfo struct {
		Address  string
		Location Location
		Error    error
	}
	PessimizeNodeStartInfo struct {
		Context  context.Context
		Address  string
		Location Location
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
		Endpoints []string
		Error     error
	}
)
