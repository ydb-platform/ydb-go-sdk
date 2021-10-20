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
		OnConnReceiveBytes func(ConnReceiveBytesStartInfo) func(ConnReceiveBytesDoneInfo)
		OnConnSendBytes    func(ConnSendBytesStartInfo) func(ConnSendBytesDoneInfo)
		OnConnNew          func(ConnNewStartInfo) func(ConnNewDoneInfo)
		OnConnClose        func(ConnCloseStartInfo) func(ConnCloseDoneInfo)
		OnConnStateChange  func(ConnStateChangeStartInfo) func(ConnStateChangeDoneInfo)
		OnConnInvoke       func(ConnInvokeStartInfo) func(ConnInvokeDoneInfo)
		OnConnNewStream    func(ConnNewStreamStartInfo) func(ConnNewStreamRecvInfo) func(ConnNewStreamDoneInfo)
		OnConnTake         func(ConnTakeStartInfo) func(ConnTakeDoneInfo)
		OnConnRelease      func(ConnReleaseStartInfo) func(ConnReleaseDoneInfo)

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
	IsValid() bool
	String() string
	Code() int
}

type endpointInfo interface {
	Address() string
	LocalDC() bool
}

type (
	ClusterInsertStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
	}
	ClusterInsertDoneInfo struct {
	}
	ClusterUpdateStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
	}
	ClusterUpdateDoneInfo struct {
		State ConnState
	}
	ClusterRemoveStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
	}
	ClusterRemoveDoneInfo struct {
		State ConnState
	}
	ConnStateChangeStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
		State    ConnState
	}
	ConnStateChangeDoneInfo struct {
		State ConnState
	}
	ConnReceiveBytesStartInfo struct {
		Address string
		Buffer  int
	}
	ConnReceiveBytesDoneInfo struct {
		Received int
		Error    error
	}
	ConnSendBytesStartInfo struct {
		Address string
		Bytes   int
	}
	ConnSendBytesDoneInfo struct {
		Sent  int
		Error error
	}
	ConnNewStartInfo struct {
		Address string
	}
	ConnNewDoneInfo struct {
		Error error
	}
	ConnTakeStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
	}
	ConnTakeDoneInfo struct {
		Lock int
	}
	ConnReleaseStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
	}
	ConnReleaseDoneInfo struct {
		Lock int
	}
	ConnCloseStartInfo struct {
		Address string
	}
	ConnCloseDoneInfo struct {
		Error error
	}
	ConnInvokeStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
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
		Endpoint endpointInfo
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
		Endpoint endpointInfo
		Error    error
	}
	PessimizeNodeStartInfo struct {
		Context  context.Context
		Endpoint endpointInfo
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
